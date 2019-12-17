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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.JdbcTest.Department;
import org.apache.calcite.test.JdbcTest.Dependent;
import org.apache.calcite.test.JdbcTest.Employee;
import org.apache.calcite.test.JdbcTest.Event;
import org.apache.calcite.test.JdbcTest.Location;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the materialized view rewrite mechanism. Each test has a
 * query and one or more materializations (what Oracle calls materialized views)
 * and checks that the materialization is used.
 */
class MaterializationTest {
  private static final Consumer<ResultSet> CONTAINS_M0 =
      CalciteAssert.checkResultContains(
          "EnumerableTableScan(table=[[hr, m0]])");

  private static final Ordering<Iterable<String>> CASE_INSENSITIVE_LIST_COMPARATOR =
      Ordering.from(String.CASE_INSENSITIVE_ORDER).lexicographical();

  private static final Ordering<Iterable<List<String>>> CASE_INSENSITIVE_LIST_LIST_COMPARATOR =
      CASE_INSENSITIVE_LIST_COMPARATOR.lexicographical();

  private static final String HR_FKUK_SCHEMA = "{\n"
      + "       type: 'custom',\n"
      + "       name: 'hr',\n"
      + "       factory: '"
      + ReflectiveSchema.Factory.class.getName()
      + "',\n"
      + "       operand: {\n"
      + "         class: '" + HrFKUKSchema.class.getName() + "'\n"
      + "       }\n"
      + "     }\n";

  private static final String HR_FKUK_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'hr',\n"
      + "   schemas: [\n"
      + HR_FKUK_SCHEMA
      + "   ]\n"
      + "}";

  final JavaTypeFactoryImpl typeFactory =
      new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RexSimplify simplify =
      new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR)
          .withParanoid(true);

  private static Sql sql() {
    return ImmutableBeans.create(Sql.class)
        .withModel(HR_FKUK_MODEL)
        .withRuleSet(RuleSets.ofList(ImmutableList.of()))
        .withChecker(CONTAINS_M0);
  }

  private static Sql sql(String materialize, String query) {
    return sql()
        .withMaterialize(materialize)
        .withQuery(query);
  }

  @Test void testScan() {
    CalciteAssert.that()
        .withMaterializations(
            "{\n"
                + "  version: '1.0',\n"
                + "  defaultSchema: 'SCOTT_CLONE',\n"
                + "  schemas: [ {\n"
                + "    name: 'SCOTT_CLONE',\n"
                + "    type: 'custom',\n"
                + "    factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',\n"
                + "    operand: {\n"
                + "      jdbcDriver: '" + JdbcTest.SCOTT.driver + "',\n"
                + "      jdbcUser: '" + JdbcTest.SCOTT.username + "',\n"
                + "      jdbcPassword: '" + JdbcTest.SCOTT.password + "',\n"
                + "      jdbcUrl: '" + JdbcTest.SCOTT.url + "',\n"
                + "      jdbcSchema: 'SCOTT'\n"
                + "   } } ]\n"
                + "}",
            "m0",
            "select empno, deptno from emp order by deptno")
        .query(
            "select empno, deptno from emp")
        .enableMaterializations(true)
        .explainContains("EnumerableTableScan(table=[[SCOTT_CLONE, m0]])")
        .sameResultWithMaterializationsDisabled();
  }

  @Test void testFilter() {
    CalciteAssert.that()
        .withMaterializations(
            HR_FKUK_MODEL,
            "m0",
            "select * from \"emps\" where \"deptno\" = 10")
        .query(
            "select \"empid\" + 1 from \"emps\" where \"deptno\" = 10")
        .enableMaterializations(true)
        .explainContains("EnumerableTableScan(table=[[hr, m0]])")
        .sameResultWithMaterializationsDisabled();
  }

  @Test void testFilterToProject0() {
    String union =
        "select * from \"emps\" where \"empid\" > 300\n"
            + "union all select * from \"emps\" where \"empid\" < 200";
    String mv = "select *, \"empid\" * 2 from (" + union + ")";
    String query = "select * from (" + union + ") where (\"empid\" * 2) > 3";
    sql(mv, query).ok();
  }

  @Test void testFilterToProject1() {
    String agg =
        "select \"deptno\", count(*) as \"c\", sum(\"salary\") as \"s\"\n"
            + "from \"emps\" group by \"deptno\"";
    String mv = "select \"c\", \"s\", \"s\" from (" + agg + ")";
    String query = "select * from (" + agg + ") where (\"s\" * 0.8) > 10000";
    sql(mv, query).noMat();
  }

  @Test void testFilterQueryOnProjectView() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(
              HR_FKUK_MODEL,
              "m0",
              "select \"deptno\", \"empid\" from \"emps\"")
          .query(
              "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10")
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  /** Checks that a given query can use a materialized view with a given
   * definition. */
  static CalciteAssert.AssertQuery checkThatMaterialize_(String materialize,
      String query, String name, boolean existing, String model,
      Consumer<ResultSet> explainChecker, final RuleSet rules,
      boolean onlyBySubstitution) {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.AssertQuery that = CalciteAssert.that()
          .withMaterializations(model, existing, name, materialize)
          .query(query)
          .enableMaterializations(true);

      // Add any additional rules required for the test
      if (rules.iterator().hasNext() || onlyBySubstitution) {
        that.withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          for (RelOptRule rule : rules) {
            planner.addRule(rule);
          }
          if (onlyBySubstitution) {
            RelOptRules.MATERIALIZATION_RULES.forEach(rule -> {
              planner.removeRule(rule);
            });
          }
        });
      }

      return that.explainMatches("", explainChecker);
    }
  }

  /** Checks that a given query CAN NOT use a materialized view with a given
   * definition. */
  private static void checkNoMaterialize_(String materialize, String query,
      String model, boolean onlyBySubstitution) {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.AssertQuery that = CalciteAssert.that()
          .withMaterializations(model, "m0", materialize)
          .query(query)
          .enableMaterializations(true);
      if (onlyBySubstitution) {
        that.withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          RelOptRules.MATERIALIZATION_RULES.forEach(rule -> {
            planner.removeRule(rule);
          });
        });
      }
      that.explainContains("EnumerableTableScan(table=[[hr, emps]])");
    }
  }

  /** Runs the same test as {@link #testFilterQueryOnProjectView()} but more
   * concisely. */
  @Test void testFilterQueryOnProjectView0() {
    sql("select \"deptno\", \"empid\" from \"emps\"",
        "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10")
        .ok();
  }

  /** As {@link #testFilterQueryOnProjectView()} but with extra column in
   * materialized view. */
  @Test void testFilterQueryOnProjectView1() {
    sql("select \"deptno\", \"empid\", \"name\" from \"emps\"",
        "select \"empid\" + 1 as x from \"emps\" where \"deptno\" = 10")
        .ok();
  }

  /** As {@link #testFilterQueryOnProjectView()} but with extra column in both
   * materialized view and query. */
  @Test void testFilterQueryOnProjectView2() {
    sql("select \"deptno\", \"empid\", \"name\" from \"emps\"",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10")
        .ok();
  }

  @Test void testFilterQueryOnProjectView3() {
    sql("select \"deptno\" - 10 as \"x\", \"empid\" + 1, \"name\" from \"emps\"",
        "select \"name\" from \"emps\" where \"deptno\" - 10 = 0")
        .ok();
  }

  /** As {@link #testFilterQueryOnProjectView3()} but materialized view cannot
   * be used because it does not contain required expression. */
  @Test void testFilterQueryOnProjectView4() {
    sql("select \"deptno\" - 10 as \"x\", \"empid\" + 1, \"name\" from \"emps\"",
        "select \"name\" from \"emps\" where \"deptno\" + 10 = 20")
        .noMat();
  }

  /** As {@link #testFilterQueryOnProjectView3()} but also contains an
   * expression column. */
  @Test void testFilterQueryOnProjectView5() {
    sql("select \"deptno\" - 10 as \"x\", \"empid\" + 1 as ee, \"name\"\n"
            + "from \"emps\"",
        "select \"name\", \"empid\" + 1 as e\n"
            + "from \"emps\" where \"deptno\" - 10 = 2")
        .withResultContains(
            "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[2], "
                + "expr#4=[=($t0, $t3)], name=[$t2], E=[$t1], $condition=[$t4])\n"
                + "  EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  /** Cannot materialize because "name" is not projected in the MV. */
  @Test void testFilterQueryOnProjectView6() {
    sql("select \"deptno\" - 10 as \"x\", \"empid\"  from \"emps\"",
        "select \"name\" from \"emps\" where \"deptno\" - 10 = 0")
        .noMat();
  }

  /** As {@link #testFilterQueryOnProjectView3()} but also contains an
   * expression column. */
  @Test void testFilterQueryOnProjectView7() {
    sql("select \"deptno\" - 10 as \"x\", \"empid\" + 1, \"name\" from \"emps\"",
        "select \"name\", \"empid\" + 2 from \"emps\" where \"deptno\" - 10 = 0")
        .noMat();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-988">[CALCITE-988]
   * FilterToProjectUnifyRule.invert(MutableRel, MutableRel, MutableProject)
   * works incorrectly</a>. */
  @Test void testFilterQueryOnProjectView8() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final String m = "select \"salary\", \"commission\",\n"
          + "\"deptno\", \"empid\", \"name\" from \"emps\"";
      final String v = "select * from \"emps\" where \"name\" is null";
      final String q = "select * from V where \"commission\" is null";
      final JsonBuilder builder = new JsonBuilder();
      final String model = "{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'hr',\n"
          + "  schemas: [\n"
          + "    {\n"
          + "      materializations: [\n"
          + "        {\n"
          + "          table: 'm0',\n"
          + "          view: 'm0v',\n"
          + "          sql: " + builder.toJsonString(m)
          + "        }\n"
          + "      ],\n"
          + "      tables: [\n"
          + "        {\n"
          + "          name: 'V',\n"
          + "          type: 'view',\n"
          + "          sql: " + builder.toJsonString(v) + "\n"
          + "        }\n"
          + "      ],\n"
          + "      type: 'custom',\n"
          + "      name: 'hr',\n"
          + "      factory: 'org.apache.calcite.adapter.java.ReflectiveSchema$Factory',\n"
          + "      operand: {\n"
          + "        class: 'org.apache.calcite.test.JdbcTest$HrSchema'\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "}\n";
      CalciteAssert.that()
          .withModel(model)
          .query(q)
          .enableMaterializations(true)
          .explainMatches("", CONTAINS_M0)
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Tag("slow")
  @Test void testFilterQueryOnFilterView() {
    sql("select \"deptno\", \"empid\", \"name\" from \"emps\" where \"deptno\" = 10",
        "select \"empid\" + 1 as x, \"name\" from \"emps\" where \"deptno\" = 10")
        .ok();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query. */
  @Test void testFilterQueryOnFilterView2() {
    final String materialize = "select \"deptno\", \"empid\", \"name\"\n"
        + "from \"emps\" where \"deptno\" = 10";
    final String query = "select \"empid\" + 1 as x, \"name\"\n"
        + "from \"emps\" where \"deptno\" = 10 and \"empid\" < 150";
    sql(materialize, query).ok();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * view. */
  @Test void testFilterQueryOnFilterView3() {
    final String materialize = "select \"deptno\", \"empid\", \"name\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" = 10 or \"deptno\" = 20 or \"empid\" < 160";
    final String query = "select \"empid\" + 1 as x, \"name\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" = 10";
    sql(materialize, query)
        .withResultContains(
            "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], expr#4=[+($t1, $t3)], expr#5=[10], "
                + "expr#6=[CAST($t0):INTEGER NOT NULL], expr#7=[=($t5, $t6)], X=[$t4], "
                + "name=[$t2], $condition=[$t7])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query. */
  @Test void testFilterQueryOnFilterView4() {
    sql("select * from \"emps\" where \"deptno\" > 10",
        "select \"name\" from \"emps\" where \"deptno\" > 30")
        .ok();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query and columns selected are subset of columns in materialized view. */
  @Test void testFilterQueryOnFilterView5() {
    sql("select \"name\", \"deptno\" from \"emps\" where \"deptno\" > 10",
        "select \"name\" from \"emps\" where \"deptno\" > 30")
        .ok();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query and columns selected are subset of columns in materialized view. */
  @Test void testFilterQueryOnFilterView6() {
    final String materialize = "select \"name\", \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"salary\" > 2000.5";
    final String query = "select \"name\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" > 30 and \"salary\" > 3000";
    sql(materialize, query).ok();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  @Test void testFilterQueryOnFilterView7() {
    final String materialize = "select * from \"emps\"\n"
        + "where ((\"salary\" < 1111.9 and \"deptno\" > 10)\n"
        + "    or (\"empid\" > 400 and \"salary\" > 5000)\n"
        + "    or \"salary\" > 500)";
    final String query = "select \"name\"\n"
        + "from \"emps\"\n"
        + "where (\"salary\" > 1000\n"
        + "  or (\"deptno\" >= 30 and \"salary\" <= 500))";
    sql(materialize, query).ok();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is stronger in
   * query. However, columns selected are not present in columns of materialized
   * view, Hence should not use materialized view. */
  @Test void testFilterQueryOnFilterView8() {
    sql("select \"name\", \"deptno\" from \"emps\" where \"deptno\" > 10",
        "select \"name\", \"empid\" from \"emps\" where \"deptno\" > 30")
        .noMat();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * query. */
  @Test void testFilterQueryOnFilterView9() {
    sql("select \"name\", \"deptno\" from \"emps\" where \"deptno\" > 10",
        "select \"name\", \"empid\" from \"emps\"\n"
            + "where \"deptno\" > 30 or \"empid\" > 10")
        .noMat();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition currently
   * has unsupported type being checked on query. */
  @Test void testFilterQueryOnFilterView10() {
    sql("select \"name\", \"deptno\" from \"emps\" where \"deptno\" > 10 "
            + "and \"name\" = \'calcite\'",
        "select \"name\", \"empid\" from \"emps\" where \"deptno\" > 30 "
            + "or \"empid\" > 10")
        .noMat();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  @Test void testFilterQueryOnFilterView11() {
    sql("select \"name\", \"deptno\" from \"emps\" where "
            + "(\"salary\" < 1111.9 and \"deptno\" > 10)"
            + "or (\"empid\" > 400 and \"salary\" > 5000)",
        "select \"name\" from \"emps\" where \"deptno\" > 30 and \"salary\" > 3000")
        .noMat();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition of
   * query is stronger but is on the column not present in MV (salary).
   */
  @Test void testFilterQueryOnFilterView12() {
    sql("select \"name\", \"deptno\" from \"emps\" where \"salary\" > 2000.5",
        "select \"name\" from \"emps\" where \"deptno\" > 30 and \"salary\" > 3000")
        .noMat();
  }

  /** As {@link #testFilterQueryOnFilterView()} but condition is weaker in
   * query and columns selected are subset of columns in materialized view.
   * Condition here is complex. */
  @Test void testFilterQueryOnFilterView13() {
    sql("select * from \"emps\" where "
            + "(\"salary\" < 1111.9 and \"deptno\" > 10)"
            + "or (\"empid\" > 400 and \"salary\" > 5000)",
        "select \"name\" from \"emps\" where \"salary\" > 1000 "
            + "or (\"deptno\" > 30 and \"salary\" > 3000)")
        .noMat();
  }

  /** As {@link #testFilterQueryOnFilterView7()} but columns in materialized
   * view are a permutation of columns in the query. */
  @Test void testFilterQueryOnFilterView14() {
    String q = "select * from \"emps\" where (\"salary\" > 1000 "
        + "or (\"deptno\" >= 30 and \"salary\" <= 500))";
    String m = "select \"deptno\", \"empid\", \"name\", \"salary\", \"commission\" "
        + "from \"emps\" as em where "
        + "((\"salary\" < 1111.9 and \"deptno\" > 10)"
        + "or (\"empid\" > 400 and \"salary\" > 5000) "
        + "or \"salary\" > 500)";
    sql(m, q).ok();
  }

  /** As {@link #testFilterQueryOnFilterView13()} but using alias
   * and condition of query is stronger. */
  @Test void testAlias() {
    sql("select * from \"emps\" as em where "
            + "(em.\"salary\" < 1111.9 and em.\"deptno\" > 10)"
            + "or (em.\"empid\" > 400 and em.\"salary\" > 5000)",
        "select \"name\" as n from \"emps\" as e where "
            + "(e.\"empid\" > 500 and e.\"salary\" > 6000)")
        .ok();
  }

  /** Aggregation query at same level of aggregation as aggregation
   * materialization. */
  @Test void testAggregate0() {
    sql("select count(*) as c from \"emps\" group by \"empid\"",
        "select count(*) + 1 as c from \"emps\" group by \"empid\"")
        .ok();
  }

  /**
   * Aggregation query at same level of aggregation as aggregation
   * materialization but with different row types. */
  @Test void testAggregate1() {
    sql("select count(*) as c0 from \"emps\" group by \"empid\"",
        "select count(*) as c1 from \"emps\" group by \"empid\"")
        .ok();
  }

  @Test void testAggregate2() {
    sql("select \"deptno\", count(*) as c, sum(\"empid\") as s from \"emps\" group by \"deptno\"",
        "select count(*) + 1 as c, \"deptno\" from \"emps\" group by \"deptno\"")
        .ok();
  }

  @Test void testAggregate3() {
    String deduplicated =
        "(select \"empid\", \"deptno\", \"name\", \"salary\", \"commission\"\n"
            + "from \"emps\"\n"
            + "group by \"empid\", \"deptno\", \"name\", \"salary\", \"commission\")";
    String mv =
        "select \"deptno\", sum(\"salary\"), sum(\"commission\"), sum(\"k\")\n"
            + "from\n"
            + "  (select \"deptno\", \"salary\", \"commission\", 100 as \"k\"\n"
            + "  from " + deduplicated + ")\n"
            + "group by \"deptno\"";
    String query =
        "select \"deptno\", sum(\"salary\"), sum(\"k\")\n"
            + "from\n"
            + "  (select \"deptno\", \"salary\", 100 as \"k\"\n"
            + "  from " + deduplicated + ")\n"
            + "group by \"deptno\"";
    sql(mv, query).ok();
  }

  @Test void testAggregate4() {
    String mv = ""
        + "select \"deptno\", \"commission\", sum(\"salary\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\", \"commission\"";
    String query = ""
        + "select \"deptno\", sum(\"salary\")\n"
        + "from \"emps\"\n"
        + "where \"commission\" = 100\n"
        + "group by \"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testAggregate5() {
    String mv = ""
        + "select \"deptno\" + \"commission\", \"commission\", sum(\"salary\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\" + \"commission\", \"commission\"";
    String query = ""
        + "select \"commission\", sum(\"salary\")\n"
        + "from \"emps\"\n"
        + "where \"commission\" * (\"deptno\" + \"commission\") = 100\n"
        + "group by \"commission\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  /**
   * Matching failed because the filtering condition under Aggregate
   * references columns for aggregation.
   */
  @Test void testAggregate6() {
    String mv = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\", sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    String query = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\"\n"
        + "from \"emps\"\n"
        + "where \"salary\" > 1000\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    sql(mv, query).withOnlyBySubstitution(true).noMat();
  }

  @Test void testAggregate7() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(
              HR_FKUK_MODEL,
              "m0",
              "select 11 as \"empno\", 22 as \"sal\", count(*) from \"emps\" group by 11, 22")
          .query(
              "select * from\n"
                  + "(select 11 as \"empno\", 22 as \"sal\", count(*)\n"
                  + "from \"emps\" group by 11, 22) tmp\n"
                  + "where \"sal\" = 33")
          .enableMaterializations(true)
          .explainContains("EnumerableValues(tuples=[[]])");
    }
  }

  /**
   * There will be a compensating Project added after matching of the Aggregate.
   * This rule targets to test if the Calc can be handled.
   */
  @Test void testCompensatingCalcWithAggregate0() {
    String mv = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\", sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    String query = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\"\n"
        + "from \"emps\"\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  /**
   * There will be a compensating Project + Filter added after matching of the Aggregate.
   * This rule targets to test if the Calc can be handled.
   */
  @Test void testCompensatingCalcWithAggregate1() {
    String mv = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\", sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    String query = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" >=20\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  /**
   * There will be a compensating Project + Filter added after matching of the Aggregate.
   * This rule targets to test if the Calc can be handled.
   */
  @Test void testCompensatingCalcWithAggregate2() {
    String mv = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\", sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "where \"deptno\" >= 10\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 10";
    String query = ""
        + "select * from\n"
        + "(select \"deptno\", sum(\"salary\") as \"sum_salary\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" >= 20\n"
        + "group by \"deptno\")\n"
        + "where \"sum_salary\" > 20";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  /** Aggregation query at same level of aggregation as aggregation
   * materialization with grouping sets. */
  @Test void testAggregateGroupSets1() {
    final String materialize = ""
        + "select \"empid\", \"deptno\", count(*) as c, sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "group by cube(\"empid\",\"deptno\")";
    final String query = "select count(*) + 1 as c, \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by cube(\"empid\",\"deptno\")";
    sql(materialize, query).ok();
  }

  /** Aggregation query with different grouping sets, should not
   * do materialization. */
  @Test void testAggregateGroupSets2() {
    final String materialize = "select \"empid\", \"deptno\",\n"
        + "  count(*) as c, sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "group by cube(\"empid\",\"deptno\")";
    final String query = "select count(*) + 1 as c, \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by rollup(\"empid\",\"deptno\")";
    sql(materialize, query).noMat();
  }

  /** Aggregation query at coarser level of aggregation than aggregation
   * materialization. Requires an additional aggregate to roll up. Note that
   * COUNT is rolled up using SUM0. */
  @Test void testAggregateRollUp() {
    final String materialize = "select \"empid\", \"deptno\", count(*) as c,\n"
        + "  sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select count(*) + 1 as c, \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by \"deptno\"";
    sql(materialize, query)
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], "
                + "expr#3=[+($t1, $t2)], C=[$t3], deptno=[$t0])\n"
                + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($2)])\n"
                + "    EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  /** Aggregation query with groupSets at coarser level of aggregation than
   * aggregation materialization. Requires an additional aggregate to roll up.
   * Note that COUNT is rolled up using SUM0. */
  @Test void testAggregateGroupSetsRollUp() {
    final String materialize = "select \"empid\", \"deptno\", count(*) as c,\n"
        + "  sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select count(*) + 1 as c,  \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by cube(\"empid\",\"deptno\")";
    final String expected = "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], "
        + "expr#4=[+($t2, $t3)], C=[$t4], deptno=[$t1])\n"
        + "  EnumerableAggregate(group=[{0, 1}], "
        + "groups=[[{0, 1}, {0}, {1}, {}]], agg#0=[$SUM0($2)])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testAggregateGroupSetsRollUp2() {
    final String materialize = "select \"empid\", \"deptno\", count(*) as c,\n"
        + "  sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select count(*) + 1 as c,  \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by cube(\"empid\",\"deptno\")";
    final String expected = "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], "
        + "expr#4=[+($t2, $t3)], C=[$t4], deptno=[$t1])\n"
        + "  EnumerableAggregate(group=[{0, 1}], "
        + "groups=[[{0, 1}, {0}, {1}, {}]], agg#0=[$SUM0($2)])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  /** Aggregation materialization with a project. */
  @Test void testAggregateProject() {
    // Note that materialization does not start with the GROUP BY columns.
    // Not a smart way to design a materialization, but people may do it.
    final String materialize = "select \"deptno\", count(*) as c,\n"
        + "  \"empid\" + 2, sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select count(*) + 1 as c, \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by \"deptno\"";
    final String expected = "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], "
        + "expr#3=[+($t1, $t2)], C=[$t3], deptno=[$t0])\n"
        + "  EnumerableAggregate(group=[{0}], agg#0=[$SUM0($1)])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3087">[CALCITE-3087]
   * AggregateOnProjectToAggregateUnifyRule ignores Project incorrectly when its
   * Mapping breaks ordering</a>. */
  @Test void testAggregateOnProject1() {
    final String materialize = "select \"empid\", \"deptno\", count(*) as c,\n"
        + "  sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select count(*) + 1 as c, \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by \"deptno\", \"empid\"";
    sql(materialize, query).ok();
  }

  @Test void testAggregateOnProject2() {
    final String materialize = "select \"empid\", \"deptno\", count(*) as c,\n"
        + "  sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select count(*) + 1 as c,  \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by cube(\"deptno\", \"empid\")";
    final String expected = "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], "
        + "expr#4=[+($t2, $t3)], C=[$t4], deptno=[$t1])\n"
        + "  EnumerableAggregate(group=[{0, 1}], "
        + "groups=[[{0, 1}, {0}, {1}, {}]], agg#0=[$SUM0($2)])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testAggregateOnProject3() {
    final String materialize = "select \"empid\", \"deptno\", count(*) as c,\n"
        + "  sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select count(*) + 1 as c, \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by rollup(\"deptno\", \"empid\")";
    final String expected = "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], "
        + "expr#4=[+($t2, $t3)], C=[$t4], deptno=[$t1])\n"
        + "  EnumerableAggregate(group=[{0, 1}], groups=[[{0, 1}, {1}, {}]], agg#0=[$SUM0($2)])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testAggregateOnProject4() {
    final String materialize = "select \"salary\", \"empid\", \"deptno\",\n"
        + "  count(*) as c, sum(\"commission\") as s\n"
        + "from \"emps\"\n"
        + "group by \"salary\", \"empid\", \"deptno\"";
    final String query = "select count(*) + 1 as c,  \"deptno\"\n"
        + "from \"emps\"\n"
        + "group by rollup(\"empid\", \"deptno\", \"salary\")";
    final String expected = "EnumerableCalc(expr#0..3=[{inputs}], expr#4=[1], "
        + "expr#5=[+($t3, $t4)], C=[$t5], deptno=[$t2])\n"
        + "  EnumerableAggregate(group=[{0, 1, 2}], "
        + "groups=[[{0, 1, 2}, {1, 2}, {1}, {}]], agg#0=[$SUM0($3)])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3448">[CALCITE-3448]
   * AggregateOnCalcToAggregateUnifyRule ignores Project incorrectly when
   * there's missing grouping or mapping breaks ordering</a>. */
  @Test void testAggregateOnProject5() {
    sql("select \"empid\", \"deptno\", \"name\", count(*) from \"emps\"\n"
            + "group by \"empid\", \"deptno\", \"name\"",
        "select \"name\", \"empid\", count(*) from \"emps\" group by \"name\", \"empid\"")
        .withResultContains(""
            + "EnumerableCalc(expr#0..2=[{inputs}], name=[$t1], empid=[$t0], EXPR$2=[$t2])\n"
            + "  EnumerableAggregate(group=[{0, 2}], EXPR$2=[$SUM0($3)])\n"
            + "    EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateOnProjectAndFilter() {
    String mv = ""
        + "select \"deptno\", sum(\"salary\"), count(1)\n"
        + "from \"emps\"\n"
        + "group by \"deptno\"";
    String query = ""
        + "select \"deptno\", count(1)\n"
        + "from \"emps\"\n"
        + "where \"deptno\" = 10\n"
        + "group by \"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testProjectOnProject() {
    String mv = ""
        + "select \"deptno\", sum(\"salary\") + 2, sum(\"commission\")\n"
        + "from \"emps\"\n"
        + "group by \"deptno\"";
    String query = ""
        + "select \"deptno\", sum(\"salary\") + 2\n"
        + "from \"emps\"\n"
        + "group by \"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testPermutationError() {
    final String materialize = "select min(\"salary\"), count(*),\n"
        + "  max(\"salary\"), sum(\"salary\"), \"empid\"\n"
        + "from \"emps\"\n"
        + "group by \"empid\"";
    final String query = "select count(*), \"empid\"\n"
        + "from \"emps\"\n"
        + "group by \"empid\"";
    sql(materialize, query)
        .withResultContains("EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinOnLeftProjectToJoin() {
    String mv = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\"), sum(\"commission\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\", count(\"name\")\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\", count(\"name\")\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testJoinOnRightProjectToJoin() {
    String mv = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\"), sum(\"commission\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\", count(\"name\")\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\"), sum(\"commission\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\"\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testJoinOnProjectsToJoin() {
    String mv = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\"), sum(\"commission\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\", count(\"name\")\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "  (select \"deptno\", sum(\"salary\")\n"
        + "  from \"emps\"\n"
        + "  group by \"deptno\") \"A\"\n"
        + "  join\n"
        + "  (select \"deptno\"\n"
        + "  from \"depts\"\n"
        + "  group by \"deptno\") \"B\"\n"
        + "  on \"A\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testJoinOnCalcToJoin0() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"A\".\"empid\", \"A\".\"deptno\", \"depts\".\"deptno\" from\n"
        + " (select \"empid\", \"deptno\" from \"emps\" where \"deptno\" > 10) A"
        + " join \"depts\"\n"
        + "on \"A\".\"deptno\" = \"depts\".\"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testJoinOnCalcToJoin1() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"B\".\"deptno\" from\n"
        + "\"emps\" join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"emps\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testJoinOnCalcToJoin2() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "(select \"empid\", \"deptno\" from \"emps\" where \"empid\" > 10) A\n"
        + "join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testJoinOnCalcToJoin3() {
    String mv = ""
        + "select \"emps\".\"empid\", \"emps\".\"deptno\", \"depts\".\"deptno\" from\n"
        + "\"emps\" join \"depts\"\n"
        + "on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = ""
        + "select * from\n"
        + "(select \"empid\", \"deptno\" + 1 as \"deptno\" from \"emps\" where \"empid\" > 10) A\n"
        + "join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" > 10) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    // Match failure because join condition references non-mapping projects.
    sql(mv, query).withOnlyBySubstitution(true).noMat();
  }

  @Test void testJoinOnCalcToJoin4() {
    String mv = "select \"emps\".\"empid\", \"emps\".\"deptno\",\n"
        + "  \"depts\".\"deptno\"\n"
        + "from \"emps\"\n"
        + "join \"depts\" on \"emps\".\"deptno\" = \"depts\".\"deptno\"";
    String query = "select *\n"
        + "from\n"
        + "(select \"empid\", \"deptno\" from \"emps\" where \"empid\" is not null) A\n"
        + "full join\n"
        + "(select \"deptno\" from \"depts\" where \"deptno\" is not null) B\n"
        + "on \"A\".\"deptno\" = \"B\".\"deptno\"";
    // Match failure because of outer join type but filtering condition in Calc is not empty.
    sql(mv, query).withOnlyBySubstitution(true).noMat();
  }

  @Tag("slow")
  @Test void testSwapJoin() {
    final String materialize = "select count(*) as c\n"
        + "from \"foodmart\".\"sales_fact_1997\" as s\n"
        + "join \"foodmart\".\"time_by_day\" as t on s.\"time_id\" = t.\"time_id\"";
    final String query = "select count(*) as c\n"
        + "from \"foodmart\".\"time_by_day\" as t\n"
        + "join \"foodmart\".\"sales_fact_1997\" as s on t.\"time_id\" = s.\"time_id\"";
    sql(materialize, query)
        .withModel(JdbcTest.FOODMART_MODEL)
        .withResultContains("EnumerableTableScan(table=[[mat, m0]])")
        .ok();
  }

  @Disabled
  @Test void testOrderByQueryOnProjectView() {
    sql("select \"deptno\", \"empid\" from \"emps\"",
        "select \"empid\" from \"emps\" order by \"deptno\"")
        .ok();
  }

  @Disabled
  @Test void testOrderByQueryOnOrderByView() {
    sql("select \"deptno\", \"empid\" from \"emps\" order by \"deptno\"",
        "select \"empid\" from \"emps\" order by \"deptno\"")
        .ok();
  }

  @Disabled
  @Test void testDifferentColumnNames() {}

  @Disabled
  @Test void testDifferentType() {}

  @Disabled
  @Test void testPartialUnion() {}

  @Disabled
  @Test void testNonDisjointUnion() {}

  @Disabled
  @Test void testMaterializationReferencesTableInOtherSchema() {}

  /** Unit test for logic functions
   * {@link org.apache.calcite.plan.SubstitutionVisitor#mayBeSatisfiable} and
   * {@link RexUtil#simplify}. */
  @Test void testSatisfiable() {
    // TRUE may be satisfiable
    checkSatisfiable(rexBuilder.makeLiteral(true), "true");

    // FALSE is not satisfiable
    checkNotSatisfiable(rexBuilder.makeLiteral(false));

    // The expression "$0 = 1".
    final RexNode i0_eq_0 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(
                typeFactory.createType(int.class), 0),
            rexBuilder.makeExactLiteral(BigDecimal.ZERO));

    // "$0 = 1" may be satisfiable
    checkSatisfiable(i0_eq_0, "=($0, 0)");

    // "$0 = 1 AND TRUE" may be satisfiable
    final RexNode e0 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeLiteral(true));
    checkSatisfiable(e0, "=($0, 0)");

    // "$0 = 1 AND FALSE" is not satisfiable
    final RexNode e1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeLiteral(false));
    checkNotSatisfiable(e1);

    // "$0 = 0 AND NOT $0 = 0" is not satisfiable
    final RexNode e2 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                i0_eq_0));
    checkNotSatisfiable(e2);

    // "TRUE AND NOT $0 = 0" may be satisfiable. Can simplify.
    final RexNode e3 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            rexBuilder.makeLiteral(true),
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                i0_eq_0));
    checkSatisfiable(e3, "<>($0, 0)");

    // The expression "$1 = 1".
    final RexNode i1_eq_1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(
                typeFactory.createType(int.class), 1),
            rexBuilder.makeExactLiteral(BigDecimal.ONE));

    // "$0 = 0 AND $1 = 1 AND NOT $0 = 0" is not satisfiable
    final RexNode e4 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                i1_eq_1,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT, i0_eq_0)));
    checkNotSatisfiable(e4);

    // "$0 = 0 AND NOT $1 = 1" may be satisfiable. Can't simplify.
    final RexNode e5 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                i1_eq_1));
    checkSatisfiable(e5, "AND(=($0, 0), <>($1, 1))");

    // "$0 = 0 AND NOT ($0 = 0 AND $1 = 1)" may be satisfiable. Can simplify.
    final RexNode e6 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.AND,
                    i0_eq_0,
                    i1_eq_1)));
    checkSatisfiable(e6, "AND(=($0, 0), <>($1, 1))");

    // "$0 = 0 AND ($1 = 1 AND NOT ($0 = 0))" is not satisfiable.
    final RexNode e7 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                i1_eq_1,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT,
                    i0_eq_0)));
    checkNotSatisfiable(e7);

    // The expression "$2".
    final RexInputRef i2 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 2);

    // The expression "$3".
    final RexInputRef i3 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 3);

    // The expression "$4".
    final RexInputRef i4 =
        rexBuilder.makeInputRef(
            typeFactory.createType(boolean.class), 4);

    // "$0 = 0 AND $2 AND $3 AND NOT ($2 AND $3 AND $4) AND NOT ($2 AND $4)" may
    // be satisfiable. Can't simplify.
    final RexNode e8 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            i0_eq_0,
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                i2,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.AND,
                    i3,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.NOT,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.AND,
                            i2,
                            i3,
                            i4)),
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.NOT,
                        i4))));
    checkSatisfiable(e8,
        "AND(=($0, 0), $2, $3, OR(NOT($2), NOT($3), NOT($4)), NOT($4))");
  }

  private void checkNotSatisfiable(RexNode e) {
    assertFalse(SubstitutionVisitor.mayBeSatisfiable(e));
    final RexNode simple = simplify.simplifyUnknownAsFalse(e);
    assertFalse(RexLiteral.booleanValue(simple));
  }

  private void checkSatisfiable(RexNode e, String s) {
    assertTrue(SubstitutionVisitor.mayBeSatisfiable(e));
    final RexNode simple = simplify.simplifyUnknownAsFalse(e);
    assertEquals(s, simple.toStringRaw());
  }

  @Test void testSplitFilter() {
    final RexLiteral i1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);
    final RexLiteral i2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(2));
    final RexLiteral i3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(3));

    final RelDataType intType = typeFactory.createType(int.class);
    final RexInputRef x = rexBuilder.makeInputRef(intType, 0); // $0
    final RexInputRef y = rexBuilder.makeInputRef(intType, 1); // $1
    final RexInputRef z = rexBuilder.makeInputRef(intType, 2); // $2

    final RexNode x_eq_1 =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, x, i1); // $0 = 1
    final RexNode x_eq_1_b =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, i1, x); // 1 = $0
    final RexNode x_eq_2 =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, x, i2); // $0 = 2
    final RexNode y_eq_2 =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, y, i2); // $1 = 2
    final RexNode z_eq_3 =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, z, i3); // $2 = 3

    final RexNode x_plus_y_gt =  // x + y > 2
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeCall(SqlStdOperatorTable.PLUS, x, y),
            i2);
    final RexNode y_plus_x_gt =  // y + x > 2
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeCall(SqlStdOperatorTable.PLUS, y, x),
            i2);

    final RexNode x_times_y_gt = // x*y > 2
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, x, y),
            i2);

    final RexNode y_times_x_gt = // 2 < y*x
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            i2,
            rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, y, x));

    final RexNode x_plus_x_gt =  // x + x > 2
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeCall(SqlStdOperatorTable.PLUS, x, y),
            i2);

    RexNode newFilter;

    // Example 1.
    //   condition: x = 1 or y = 2
    //   target:    y = 2 or 1 = x
    // yields
    //   residue:   true
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2),
        rexBuilder.makeCall(SqlStdOperatorTable.OR, y_eq_2, x_eq_1_b));
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // Example 2.
    //   condition: x = 1,
    //   target:    x = 1 or z = 3
    // yields
    //   residue:   x = 1
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, z_eq_3));
    assertThat(newFilter.toStringRaw(), equalTo("=($0, 1)"));

    // 2b.
    //   condition: x = 1 or y = 2
    //   target:    x = 1 or y = 2 or z = 3
    // yields
    //   residue:   x = 1 or y = 2
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2),
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2, z_eq_3));
    assertThat(newFilter.toStringRaw(), equalTo("OR(=($0, 1), =($1, 2))"));

    // 2c.
    //   condition: x = 1
    //   target:    x = 1 or y = 2 or z = 3
    // yields
    //   residue:   x = 1
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2, z_eq_3));
    assertThat(newFilter.toStringRaw(),
        equalTo("=($0, 1)"));

    // 2d.
    //   condition: x = 1 or y = 2
    //   target:    y = 2 or x = 1
    // yields
    //   residue:   true
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2),
        rexBuilder.makeCall(SqlStdOperatorTable.OR, y_eq_2, x_eq_1));
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // 2e.
    //   condition: x = 1
    //   target:    x = 1 (different object)
    // yields
    //   residue:   true
    newFilter = SubstitutionVisitor.splitFilter(simplify, x_eq_1, x_eq_1_b);
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // 2f.
    //   condition: x = 1 or y = 2
    //   target:    x = 1
    // yields
    //   residue:   null
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.OR, x_eq_1, y_eq_2),
        x_eq_1);
    assertNull(newFilter);

    // Example 3.
    // Condition [x = 1 and y = 2],
    // target [y = 2 and x = 1] yields
    // residue [true].
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.AND, x_eq_1, y_eq_2),
        rexBuilder.makeCall(SqlStdOperatorTable.AND, y_eq_2, x_eq_1));
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // Example 4.
    //   condition: x = 1 and y = 2
    //   target:    y = 2
    // yields
    //   residue:   x = 1
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        rexBuilder.makeCall(SqlStdOperatorTable.AND, x_eq_1, y_eq_2),
        y_eq_2);
    assertThat(newFilter.toStringRaw(), equalTo("=($0, 1)"));

    // Example 5.
    //   condition: x = 1
    //   target:    x = 1 and y = 2
    // yields
    //   residue:   null
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        rexBuilder.makeCall(SqlStdOperatorTable.AND, x_eq_1, y_eq_2));
    assertNull(newFilter);

    // Example 6.
    //   condition: x = 1
    //   target:    y = 2
    // yields
    //   residue:   null
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        y_eq_2);
    assertNull(newFilter);

    // Example 7.
    //   condition: x = 1
    //   target:    x = 2
    // yields
    //   residue:   null
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_eq_1,
        x_eq_2);
    assertNull(newFilter);

    // Example 8.
    //   condition: x + y > 2
    //   target:    y + x > 2
    // yields
    //   residue:  true
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_plus_y_gt,
        y_plus_x_gt);
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // Example 9.
    //   condition: x + x > 2
    //   target:    x + x > 2
    // yields
    //   residue:  true
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_plus_x_gt,
        x_plus_x_gt);
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));

    // Example 10.
    //   condition: x * y > 2
    //   target:    2 < y * x
    // yields
    //   residue:  true
    newFilter = SubstitutionVisitor.splitFilter(simplify,
        x_times_y_gt,
        y_times_x_gt);
    assertThat(newFilter.isAlwaysTrue(), equalTo(true));
  }

  /** Tests a complicated star-join query on a complicated materialized
   * star-join query. Some of the features:
   *
   * <ol>
   * <li>query joins in different order;
   * <li>query's join conditions are in where clause;
   * <li>query does not use all join tables (safe to omit them because they are
   *    many-to-mandatory-one joins);
   * <li>query is at higher granularity, therefore needs to roll up;
   * <li>query has a condition on one of the materialization's grouping columns.
   * </ol>
   */
  @Disabled
  @Test void testFilterGroupQueryOnStar() {
    sql("select p.\"product_name\", t.\"the_year\",\n"
            + "  sum(f.\"unit_sales\") as \"sum_unit_sales\", count(*) as \"c\"\n"
            + "from \"foodmart\".\"sales_fact_1997\" as f\n"
            + "join (\n"
            + "    select \"time_id\", \"the_year\", \"the_month\"\n"
            + "    from \"foodmart\".\"time_by_day\") as t\n"
            + "  on f.\"time_id\" = t.\"time_id\"\n"
            + "join \"foodmart\".\"product\" as p\n"
            + "  on f.\"product_id\" = p.\"product_id\"\n"
            + "join \"foodmart\".\"product_class\" as pc"
            + "  on p.\"product_class_id\" = pc.\"product_class_id\"\n"
            + "group by t.\"the_year\",\n"
            + " t.\"the_month\",\n"
            + " pc.\"product_department\",\n"
            + " pc.\"product_category\",\n"
            + " p.\"product_name\"",
        "select t.\"the_month\", count(*) as x\n"
            + "from (\n"
            + "  select \"time_id\", \"the_year\", \"the_month\"\n"
            + "  from \"foodmart\".\"time_by_day\") as t,\n"
            + " \"foodmart\".\"sales_fact_1997\" as f\n"
            + "where t.\"the_year\" = 1997\n"
            + "and t.\"time_id\" = f.\"time_id\"\n"
            + "group by t.\"the_year\",\n"
            + " t.\"the_month\"\n")
        .withModel(JdbcTest.FOODMART_MODEL)
        .ok();
  }

  /** Simpler than {@link #testFilterGroupQueryOnStar()}, tests a query on a
   * materialization that is just a join. */
  @Disabled
  @Test void testQueryOnStar() {
    String q = "select *\n"
        + "from \"foodmart\".\"sales_fact_1997\" as f\n"
        + "join \"foodmart\".\"time_by_day\" as t on f.\"time_id\" = t.\"time_id\"\n"
        + "join \"foodmart\".\"product\" as p on f.\"product_id\" = p.\"product_id\"\n"
        + "join \"foodmart\".\"product_class\" as pc on p.\"product_class_id\" = pc.\"product_class_id\"\n";
    sql(q, q + "where t.\"month_of_year\" = 10")
        .withModel(JdbcTest.FOODMART_MODEL)
        .ok();
  }

  /** A materialization that is a join of a union cannot at present be converted
   * to a star table and therefore cannot be recognized. This test checks that
   * nothing unpleasant happens. */
  @Disabled
  @Test void testJoinOnUnionMaterialization() {
    String q = "select *\n"
        + "from (select * from \"emps\" union all select * from \"emps\")\n"
        + "join \"depts\" using (\"deptno\")";
    sql(q, q).noMat();
  }

  @Test void testJoinMaterialization() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join \"depts\" using (\"deptno\")";
    sql("select * from \"emps\" where \"empid\" < 500", q).ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-891">[CALCITE-891]
   * TableScan without Project cannot be substituted by any projected
   * materialization</a>. */
  @Test void testJoinMaterialization2() {
    String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\")";
    final String m = "select \"deptno\", \"empid\", \"name\",\n"
        + "\"salary\", \"commission\" from \"emps\"";
    sql(m, q).ok();
  }

  @Test void testJoinMaterialization3() {
    String q = "select \"empid\" \"deptno\" from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" = 1";
    final String m = "select \"empid\" \"deptno\" from \"emps\"\n"
        + "join \"depts\" using (\"deptno\")";
    sql(m, q).ok();
  }

  @Test void testUnionAll() {
    String q = "select * from \"emps\" where \"empid\" > 300\n"
        + "union all select * from \"emps\" where \"empid\" < 200";
    String m = "select * from \"emps\" where \"empid\" < 500";
    sql(m, q).withChecker(
        CalciteAssert.checkResultContains(
            "EnumerableTableScan(table=[[hr, m0]])", 1))
        .ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs1() {
    sql("select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"",
        "select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"")
        .withResultContains(
            "EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs2() {
    sql("select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{1}])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs3() {
    sql("select \"deptno\" from \"emps\" group by \"deptno\"",
        "select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"")
        .noMat();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs4() {
    final String materialize = "select \"empid\", \"deptno\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" = 10\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select \"deptno\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" = 10\n"
        + "group by \"deptno\"";
    final String expected = "EnumerableAggregate(group=[{1}])\n"
        + "  EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs5() {
    final String materialize = "select \"empid\", \"deptno\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" = 5\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select \"deptno\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" = 10\n"
        + "group by \"deptno\"";
    sql(materialize, query).noMat();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs6() {
    final String materialize = "select \"empid\", \"deptno\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" > 5\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select \"deptno\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" > 10\n"
        + "group by \"deptno\"";
    final String expected = "EnumerableAggregate(group=[{1}])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[10], expr#3=[>($t1, $t2)], "
        + "proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs7() {
    final String materialize = "select \"empid\", \"deptno\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" > 5\n"
        + "group by \"empid\", \"deptno\"";
    final String query = "select \"deptno\"\n"
        + "from \"emps\"\n"
        + "where \"deptno\" < 10\n"
        + "group by \"deptno\"";
    sql(materialize, query).noMat();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs8() {
    sql("select \"empid\" from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"")
        .noMat();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs9() {
    sql("select \"empid\", \"deptno\" from \"emps\"\n"
            + "where \"salary\" > 1000 group by \"name\", \"empid\", \"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "where \"salary\" > 2000 group by \"name\", \"empid\"")
        .noMat();
  }

  @Test void testAggregateMaterializationAggregateFuncs1() {
    sql("select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{1}])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs2() {
    sql("select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{1}], C=[$SUM0($2)], S=[$SUM0($3)])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs3() {
    sql("select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\", \"empid\", sum(\"empid\") as s, count(*) as c\n"
            + "from \"emps\" group by \"empid\", \"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0..3=[{inputs}], deptno=[$t1], empid=[$t0], "
                + "S=[$t3], C=[$t2])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs4() {
    sql("select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{1}], S=[$SUM0($3)])\n"
                + "  EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[>($t1, $t4)], "
                + "proj#0..3=[{exprs}], $condition=[$t5])\n"
                + "    EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs5() {
    sql("select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\", sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)],"
                + " deptno=[$t0], S=[$t3])\n"
                + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
                + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
                + "proj#0..3=[{exprs}], $condition=[$t5])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs6() {
    sql("select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") + 2 as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\", sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"")
        .noMat();
  }

  @Test void testAggregateMaterializationAggregateFuncs7() {
    sql("select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\" + 1, sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t0, $t2)], "
                + "expr#4=[+($t1, $t2)], EXPR$0=[$t3], S=[$t4])\n"
                + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
                + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
                + "proj#0..3=[{exprs}], $condition=[$t5])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Disabled
  @Test void testAggregateMaterializationAggregateFuncs8() {
    // TODO: It should work, but top project in the query is not matched by the planner.
    // It needs further checking.
    sql("select \"empid\", \"deptno\" + 1, count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\" + 1, sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs9() {
    final String materialize = "select \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to month),\n"
        + "  count(*) + 1 as c, sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to month)";
    final String query = "select\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to year),\n"
        + "  sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)";
    sql(materialize, query).ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs10() {
    final String materialize = "select \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to month),\n"
        + "  count(*) + 1 as c, sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to month)";
    final String query = "select\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to year),\n"
        + " sum(\"empid\") + 1 as s\n"
        + "from \"emps\"\n"
        + "group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)";
    sql(materialize, query).ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs11() {
    final String materialize = "select \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to second),\n"
        + "  count(*) + 1 as c, sum(\"empid\") as s\nfrom \"emps\"\n"
        + "group by \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to second)";
    final String query = "select\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to minute),\n"
        + "  sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by floor(cast('1997-01-20 12:34:56' as timestamp) to minute)";
    sql(materialize, query).ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs12() {
    final String materialize = "select \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to second),\n"
        + "  count(*) + 1 as c, sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to second)";
    final String query = "select\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to month),\n"
        + "  sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by floor(cast('1997-01-20 12:34:56' as timestamp) to month)";
    sql(materialize, query).ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs13() {
    final String materialize = "select \"empid\",\n"
        + "  cast('1997-01-20 12:34:56' as timestamp),\n"
        + "  count(*) + 1 as c, sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\", cast('1997-01-20 12:34:56' as timestamp)";
    final String query = "select\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to year),\n"
        + "  sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)";
    sql(materialize, query).ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs14() {
    final String materialize = "select \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to month),\n"
        + "  count(*) + 1 as c, sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by \"empid\",\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to month)";
    final String query = "select\n"
        + "  floor(cast('1997-01-20 12:34:56' as timestamp) to hour),\n"
        + "  sum(\"empid\") as s\n"
        + "from \"emps\"\n"
        + "group by floor(cast('1997-01-20 12:34:56' as timestamp) to hour)";
    sql(materialize, query).ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs15() {
    final String materialize = "select \"eventid\",\n"
        + "  floor(cast(\"ts\" as timestamp) to second), count(*) + 1 as c,\n"
        + "  sum(\"eventid\") as s\n"
        + "from \"events\"\n"
        + "group by \"eventid\", floor(cast(\"ts\" as timestamp) to second)";
    final String query = "select floor(cast(\"ts\" as timestamp) to minute),\n"
        + "  sum(\"eventid\") as s\n"
        + "from \"events\"\n"
        + "group by floor(cast(\"ts\" as timestamp) to minute)";
    sql(materialize, query).ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs16() {
    sql("select \"eventid\", cast(\"ts\" as timestamp), count(*) + 1 as c, sum(\"eventid\") as s\n"
            + "from \"events\" group by \"eventid\", cast(\"ts\" as timestamp)",
        "select floor(cast(\"ts\" as timestamp) to year), sum(\"eventid\") as s\n"
            + "from \"events\" group by floor(cast(\"ts\" as timestamp) to year)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs17() {
    final String materialize = "select \"eventid\",\n"
        + "  floor(cast(\"ts\" as timestamp) to month), count(*) + 1 as c,\n"
        + "  sum(\"eventid\") as s\n"
        + "from \"events\"\n"
        + "group by \"eventid\", floor(cast(\"ts\" as timestamp) to month)";
    final String query = "select floor(cast(\"ts\" as timestamp) to hour),\n"
        + "  sum(\"eventid\") as s\n"
        + "from \"events\"\n"
        + "group by floor(cast(\"ts\" as timestamp) to hour)";
    final String expected = "EnumerableTableScan(table=[[hr, events]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs18() {
    sql("select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"empid\"*\"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\"*\"deptno\"")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs19() {
    sql("select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"empid\" + 10, count(*) + 1 as c\n"
            + "from \"emps\" group by \"empid\" + 10")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs1() {
    sql("select \"empid\", \"depts\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[>($t1, $t2)], "
                + "empid=[$t0], $condition=[$t3])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs2() {
    sql("select \"depts\".\"deptno\", \"empid\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"depts\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[<($t2, $t0)], "
                + "empid=[$t1], $condition=[$t3])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs3() {
    // It does not match, Project on top of query
    sql("select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"")
        .noMat();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs4() {
    sql("select \"empid\", \"depts\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"emps\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[<($t2, $t1)], "
                + "empid=[$t0], $condition=[$t3])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs5() {
    final String materialize = "select \"depts\".\"deptno\", \"emps\".\"empid\" from \"depts\"\n"
        + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 10\n"
        + "group by \"depts\".\"deptno\", \"emps\".\"empid\"";
    final String query = "select \"depts\".\"deptno\" from \"depts\"\n"
        + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 15\n"
        + "group by \"depts\".\"deptno\", \"emps\".\"empid\"";
    final String expected = ""
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[15], expr#3=[>($t1, $t2)], "
        + "deptno=[$t0], $condition=[$t3])\n"
        + "  EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs6() {
    final String materialize = "select \"depts\".\"deptno\", \"emps\".\"empid\" from \"depts\"\n"
        + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 10\n"
        + "group by \"depts\".\"deptno\", \"emps\".\"empid\"";
    final String query = "select \"depts\".\"deptno\" from \"depts\"\n"
        + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 15\n"
        + "group by \"depts\".\"deptno\"";
    final String expected = "EnumerableAggregate(group=[{0}])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[15], expr#3=[>($t1, $t2)], "
        + "proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Tag("slow")
  @Test void testJoinAggregateMaterializationNoAggregateFuncs7() {
    final String materialize = "select \"depts\".\"deptno\",\n"
        + " \"dependents\".\"empid\"\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 11\n"
        + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"";
    final String query = "select \"dependents\".\"empid\"\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 10\n"
        + "group by \"dependents\".\"empid\"";
    final String[] expecteds = {
        "EnumerableAggregate(group=[{0}])",
        "EnumerableUnion(all=[true])",
        "EnumerableAggregate(group=[{2}])",
        "EnumerableTableScan(table=[[hr, m0]])",
        "expr#5=[10], expr#6=[>($t0, $t5)], expr#7=[11], expr#8=[>=($t7, $t0)]"};
    sql(materialize, query).withResultContains(expecteds).ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs8() {
    final String materialize = "select \"depts\".\"deptno\",\n"
        + " \"dependents\".\"empid\"\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 20\n"
        + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"";
    final String query = "select \"dependents\".\"empid\"\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
        + "group by \"dependents\".\"empid\"";
    sql(materialize, query).noMat();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs9() {
    final String materialize = "select \"depts\".\"deptno\",\n"
        + " \"dependents\".\"empid\"\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
        + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"";
    final String query = "select \"dependents\".\"empid\"\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
        + "group by \"dependents\".\"empid\"";
    final String[] expecteds = {
        "EnumerableAggregate(group=[{0}])",
        "EnumerableUnion(all=[true])",
        "EnumerableAggregate(group=[{2}])",
        "EnumerableTableScan(table=[[hr, m0]])",
        "expr#13=[OR($t10, $t12)], expr#14=[AND($t6, $t8, $t13)]"};
    sql(materialize, query).withResultContains(expecteds).ok();
  }

  @Tag("slow")
  @Test void testJoinAggregateMaterializationNoAggregateFuncs10() {
    final String materialize = "select \"depts\".\"name\", \"dependents\".\"name\" as \"name2\", "
        + "\"emps\".\"deptno\", \"depts\".\"deptno\" as \"deptno2\", "
        + "\"dependents\".\"empid\"\n"
        + "from \"depts\", \"dependents\", \"emps\"\n"
        + "where \"depts\".\"deptno\" > 10\n"
        + "group by \"depts\".\"name\", \"dependents\".\"name\", "
        + "\"emps\".\"deptno\", \"depts\".\"deptno\", "
        + "\"dependents\".\"empid\"";
    final String query = "select \"dependents\".\"empid\"\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 10\n"
        + "group by \"dependents\".\"empid\"";
    final String expected = "EnumerableAggregate(group=[{4}])\n"
        + "  EnumerableCalc(expr#0..4=[{inputs}], expr#5=[=($t2, $t3)], "
        + "expr#6=[CAST($t1):VARCHAR], expr#7=[CAST($t0):VARCHAR], "
        + "expr#8=[=($t6, $t7)], expr#9=[AND($t5, $t8)], proj#0..4=[{exprs}], "
        + "$condition=[$t9])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs1() {
    // This test relies on FK-UK relationship
    final String materialize =
        "select \"empid\", \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"depts\".\"deptno\"";
    final String query = "select \"deptno\" from \"emps\" group by \"deptno\"";
    final String expected = "EnumerableAggregate(group=[{1}])\n"
        + "  EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs2() {
    final String materialize =
        "select \"empid\", \"emps\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"emps\".\"deptno\"";
    final String query = "select \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
        + "from \"emps\" join \"depts\" using (\"deptno\")\n"
        + "group by \"depts\".\"deptno\"";
    final String expected = "EnumerableAggregate(group=[{1}], C=[$SUM0($2)], S=[$SUM0($3)])\n"
        + "  EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs3() {
    // This test relies on FK-UK relationship
    final String materialize =
        "select \"empid\", \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"depts\".\"deptno\"";
    final String query = "select \"deptno\", \"empid\", sum(\"empid\") as s, count(*) as c\n"
        + "from \"emps\" group by \"empid\", \"deptno\"";
    final String expected = "EnumerableCalc(expr#0..3=[{inputs}], "
        + "deptno=[$t1], empid=[$t0], S=[$t3], C=[$t2])\n"
            + "  EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs4() {
    final String materialize =
        "select \"empid\", \"emps\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"emps\".\"deptno\" >= 10 group by \"empid\", \"emps\".\"deptno\"";
    final String query = "select \"depts\".\"deptno\", sum(\"empid\") as s\n"
        + "from \"emps\" join \"depts\" using (\"deptno\")\n"
        + "where \"emps\".\"deptno\" > 10 group by \"depts\".\"deptno\"";
    final String expected = "EnumerableAggregate(group=[{1}], S=[$SUM0($3)])\n"
        + "  EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
        + "proj#0..3=[{exprs}], $condition=[$t5])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs5() {
    final String materialize =
        "select \"empid\", \"depts\".\"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"depts\".\"deptno\" >= 10 group by \"empid\", \"depts\".\"deptno\"";
    final String query = "select \"depts\".\"deptno\", sum(\"empid\") + 1 as s\n"
        + "from \"emps\" join \"depts\" using (\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 10 group by \"depts\".\"deptno\"";
    final String expected = "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], "
        + "expr#3=[+($t1, $t2)], deptno=[$t0], S=[$t3])\n"
        + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
        + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
        + "proj#0..3=[{exprs}], $condition=[$t5])\n"
        + "      EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Disabled
  @Test void testJoinAggregateMaterializationAggregateFuncs6() {
    // This rewriting would be possible if planner generates a pre-aggregation,
    // since the materialized view would match the sub-query.
    // Initial investigation after enabling AggregateJoinTransposeRule.EXTENDED
    // shows that the rewriting with pre-aggregations is generated and the
    // materialized view rewriting happens.
    // However, we end up discarding the plan with the materialized view and still
    // using the plan with the pre-aggregations.
    // TODO: Explore and extend to choose best rewriting.
    final String m = "select \"depts\".\"name\", sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "group by \"depts\".\"name\"";
    final String q = "select \"dependents\".\"empid\", sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "group by \"dependents\".\"empid\"";
    sql(m, q).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs7() {
    final String materialize = "select \"dependents\".\"empid\",\n"
        + "  \"emps\".\"deptno\", sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"";
    final String query = "select \"dependents\".\"empid\", sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"dependents\".\"empid\"";
    final String expected = "EnumerableAggregate(group=[{0}], S=[$SUM0($2)])\n"
        + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])\n"
        + "    EnumerableTableScan(table=[[hr, depts]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs8() {
    final String materialize = "select \"dependents\".\"empid\",\n"
        + "  \"emps\".\"deptno\", sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"";
    final String query = "select \"depts\".\"name\", sum(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"depts\".\"name\"";
    final String expected = "EnumerableAggregate(group=[{4}], S=[$SUM0($2)])\n"
        + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
        + "    EnumerableTableScan(table=[[hr, m0]])\n"
        + "    EnumerableTableScan(table=[[hr, depts]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs9() {
    final String materialize = "select \"dependents\".\"empid\",\n"
        + "  \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"";
    final String query = "select \"emps\".\"deptno\",\n"
        + "  count(distinct \"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"";
    final String expected = "EnumerableCalc(expr#0..2=[{inputs}], "
        + "deptno=[$t1], S=[$t2])\n"
        + "  EnumerableTableScan(table=[[hr, m0]])";
    sql(materialize, query).withResultContains(expected).ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs10() {
    final String materialize = "select \"dependents\".\"empid\",\n"
        + "  \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"";
    final String query = "select \"emps\".\"deptno\",\n"
        + "  count(distinct \"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"emps\".\"deptno\"";
    sql(materialize, query).noMat();
  }

  @Tag("slow")
  @Test void testJoinAggregateMaterializationAggregateFuncs11() {
    final String materialize = "select \"depts\".\"deptno\",\n"
        + "  \"dependents\".\"empid\", count(\"emps\".\"salary\") as s\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
        + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"";
    final String query = "select \"dependents\".\"empid\",\n"
        + "  count(\"emps\".\"salary\") + 1\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
        + "group by \"dependents\".\"empid\"";
    sql(materialize, query)
        .withResultContains(
            "PLAN=EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], "
                + "empid=[$t0], EXPR$1=[$t3])\n"
                + "  EnumerableAggregate(group=[{0}], agg#0=[$SUM0($1)])",
            "EnumerableUnion(all=[true])",
            "EnumerableAggregate(group=[{2}], agg#0=[COUNT()])",
            "EnumerableAggregate(group=[{1}], agg#0=[$SUM0($2)])",
            "EnumerableTableScan(table=[[hr, m0]])",
            "expr#13=[OR($t10, $t12)], expr#14=[AND($t6, $t8, $t13)]")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs12() {
    final String materialize = "select \"depts\".\"deptno\",\n"
        + "  \"dependents\".\"empid\",\n"
        + "  count(distinct \"emps\".\"salary\") as s\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
        + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"";
    final String query = "select \"dependents\".\"empid\",\n"
        + "  count(distinct \"emps\".\"salary\") + 1\n"
        + "from \"depts\"\n"
        + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
        + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
        + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
        + "group by \"dependents\".\"empid\"";
    sql(materialize, query).noMat();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs13() {
    final String materialize = "select \"dependents\".\"empid\",\n"
        + "  \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"";
    final String query = "select \"emps\".\"deptno\", count(\"salary\") as s\n"
        + "from \"emps\"\n"
        + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
        + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"";
    sql(materialize, query).noMat();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs14() {
    sql("select \"empid\", \"emps\".\"name\", \"emps\".\"deptno\", \"depts\".\"name\", "
            + "count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b')\n"
            + "group by \"empid\", \"emps\".\"name\", \"depts\".\"name\", \"emps\".\"deptno\"",
        "select \"depts\".\"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"depts\".\"name\" is not null and \"emps\".\"name\" = 'a'\n"
            + "group by \"depts\".\"deptno\"")
        .ok();
  }

  @Test void testJoinMaterialization4() {
    sql("select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" = 1")
        .withResultContains(
            "EnumerableCalc(expr#0=[{inputs}], expr#1=[1], expr#2=[CAST($t0):INTEGER NOT NULL], "
                + "expr#3=[=($t1, $t2)], deptno=[$t0], $condition=[$t3])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterialization5() {
    sql("select cast(\"empid\" as BIGINT) from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" > 1")
        .withResultContains(
            "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):JavaType(int) NOT NULL], "
                + "expr#2=[1], expr#3=[<($t2, $t1)], EXPR$0=[$t1], $condition=[$t3])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterialization6() {
    sql("select cast(\"empid\" as BIGINT) from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" = 1")
        .withResultContains(
            "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):JavaType(int) NOT NULL], "
                + "expr#2=[1], expr#3=[CAST($t1):INTEGER NOT NULL], expr#4=[=($t2, $t3)], "
                + "EXPR$0=[$t1], $condition=[$t4])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterialization7() {
    sql("select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")")
        .withResultContains(
            "EnumerableCalc(expr#0..2=[{inputs}], empid=[$t1])\n"
                + "  EnumerableHashJoin(condition=[=($0, $2)], joinType=[inner])\n"
                + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR], name00=[$t1])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])\n"
                + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CAST($t1):VARCHAR], empid=[$t0], name0=[$t2])\n"
                + "      EnumerableTableScan(table=[[hr, dependents]])")
        .ok();
  }

  @Test void testJoinMaterialization8() {
    sql("select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")")
        .withResultContains(
            "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t2])\n"
                + "  EnumerableHashJoin(condition=[=($1, $4)], joinType=[inner])\n"
                + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR], proj#0..1=[{exprs}])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])\n"
                + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CAST($t1):VARCHAR], proj#0..2=[{exprs}])\n"
                + "      EnumerableTableScan(table=[[hr, dependents]])")
        .ok();
  }

  @Test void testJoinMaterialization9() {
    sql("select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")")
        .ok();
  }

  @Tag("slow")
  @Test void testJoinMaterialization10() {
    sql("select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 30",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10")
        .withResultContains(
            "EnumerableUnion(all=[true])",
            "EnumerableTableScan(table=[[hr, m0]])",
            "expr#5=[10], expr#6=[>($t0, $t5)], expr#7=[30], expr#8=[>=($t7, $t0)]")
        .ok();
  }

  @Test void testJoinMaterialization11() {
    sql("select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" from \"emps\"\n"
            + "where \"deptno\" in (select \"deptno\" from \"depts\")")
        .withResultContains(
            "PLAN=EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Tag("slow")
  @Test void testJoinMaterialization12() {
    sql("select \"empid\", \"emps\".\"name\", \"emps\".\"deptno\", \"depts\".\"name\"\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'c')",
        "select \"depts\".\"deptno\", \"depts\".\"name\"\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where (\"depts\".\"name\" is not null and \"emps\".\"name\" = 'a') or "
            + "(\"depts\".\"name\" is not null and \"emps\".\"name\" = 'b')")
        .ok();
  }

  @Test void testJoinMaterializationUKFK1() {
    sql("select \"a\".\"empid\" \"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"empid\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n")
        .withResultContains(
            "PLAN=EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterializationUKFK2() {
    sql("select \"a\".\"empid\", \"a\".\"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"empid\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterializationUKFK3() {
    sql("select \"a\".\"empid\", \"a\".\"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"name\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n")
        .noMat();
  }

  @Test void testJoinMaterializationUKFK4() {
    sql("select \"empid\" \"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1)\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" from \"emps\" where \"empid\" = 1\n")
        .withResultContains(
            "PLAN=EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Tag("slow")
  @Test void testJoinMaterializationUKFK5() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Tag("slow")
  @Test void testJoinMaterializationUKFK6() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"deptno\"=\"b\".\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
                + "  EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testJoinMaterializationUKFK7() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"name\"=\"a\".\"name\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"name\"=\"b\".\"name\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .noMat();
  }

  @Test void testJoinMaterializationUKFK8() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"name\"=\"b\".\"name\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .noMat();
  }

  @Tag("slow")
  @Test void testJoinMaterializationUKFK9() {
    sql("select * from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"emps\".\"empid\", \"dependents\".\"empid\", \"emps\".\"deptno\"\n"
            + "from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "where \"emps\".\"name\" = 'Bill'")
        .withResultContains(
            "EnumerableTableScan(table=[[hr, m0]])")
        .ok();
  }

  @Test void testAggregateOnJoinKeys() {
    sql("select \"deptno\", \"empid\", \"salary\" "
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"empid\", \"depts\".\"deptno\" "
            + "from \"emps\"\n"
            + "join \"depts\" on \"depts\".\"deptno\" = \"empid\" group by \"empid\", \"depts\".\"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0=[{inputs}], empid=[$t0], empid0=[$t0])\n"
                + "  EnumerableAggregate(group=[{1}])\n"
                + "    EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])\n"
                + "      EnumerableTableScan(table=[[hr, depts]])")
        .ok();
  }

  @Test void testAggregateOnJoinKeys2() {
    sql("select \"deptno\", \"empid\", \"salary\", sum(1) "
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select sum(1) "
            + "from \"emps\"\n"
            + "join \"depts\" on \"depts\".\"deptno\" = \"empid\" group by \"empid\", \"depts\".\"deptno\"")
        .withResultContains(
            "EnumerableCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n"
                + "  EnumerableAggregate(group=[{1}], EXPR$0=[$SUM0($3)])\n"
                + "    EnumerableHashJoin(condition=[=($1, $4)], joinType=[inner])\n"
                + "      EnumerableTableScan(table=[[hr, m0]])\n"
                + "      EnumerableTableScan(table=[[hr, depts]])")
        .ok();
  }

  @Test void testViewMaterialization() {
    sql("select \"depts\".\"name\"\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")",
        "select \"depts\".\"name\"\n"
            + "from \"depts\"\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")")
        .withViewName("matview")
        .withExisting(true)
        .withResultContains(
            "EnumerableValues(tuples=[[{ 'noname' }]])")
        .that()
        .returnsValue("noname");
  }

  @Test void testSubQuery() {
    String q = "select \"empid\", \"deptno\", \"salary\" from \"emps\" e1\n"
        + "where \"empid\" = (\n"
        + "  select max(\"empid\") from \"emps\"\n"
        + "  where \"deptno\" = e1.\"deptno\")";
    final String m = "select \"empid\", \"deptno\" from \"emps\"\n";
    sql(m, q).withChecker(
        CalciteAssert.checkResultContains(
            "EnumerableTableScan(table=[[hr, m0]])", 1))
        .ok();
  }

  @Test void testTableModify() {
    final String m = "select \"deptno\", \"empid\", \"name\""
        + "from \"emps\" where \"deptno\" = 10";
    final String q = "upsert into \"dependents\""
        + "select \"empid\" + 1 as x, \"name\""
        + "from \"emps\" where \"deptno\" = 10";

    final List<List<List<String>>> substitutedNames = new ArrayList<>();
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", m)
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .explainContains("hr, m0");
    } catch (Exception e) {
      // Table "dependents" not modifiable.
    }
    assertThat(substitutedNames, is(list3(new String[][][]{{{"hr", "m0"}}})));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-761">[CALCITE-761]
   * Pre-populated materializations</a>. */
  @Test void testPrePopulated() {
    String q = "select distinct \"deptno\" from \"emps\"";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(
              HR_FKUK_MODEL, builder -> {
                final Map<String, Object> map = builder.map();
                map.put("table", "locations");
                String sql = "select distinct `deptno` as `empid`, '' as `name`\n"
                    + "from `emps`";
                final String sql2 = sql.replace("`", "\"");
                map.put("sql", sql2);
                return ImmutableList.of(map);
              })
          .query(q)
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testViewSchemaPath() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final String m = "select empno, deptno from emp";
      final String q = "select deptno from scott.emp";
      final List<String> path = ImmutableList.of("SCOTT");
      final JsonBuilder builder = new JsonBuilder();
      final String model = "{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'hr',\n"
          + "  schemas: [\n"
          + JdbcTest.SCOTT_SCHEMA
          + "  ,\n"
          + "    {\n"
          + "      materializations: [\n"
          + "        {\n"
          + "          table: 'm0',\n"
          + "          view: 'm0v',\n"
          + "          sql: " + builder.toJsonString(m) + ",\n"
          + "          viewSchemaPath: " + builder.toJsonString(path)
          + "        }\n"
          + "      ],\n"
          + "      type: 'custom',\n"
          + "      name: 'hr',\n"
          + "      factory: 'org.apache.calcite.adapter.java.ReflectiveSchema$Factory',\n"
          + "      operand: {\n"
          + "        class: 'org.apache.calcite.test.JdbcTest$HrSchema'\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "}\n";
      CalciteAssert.that()
          .withModel(model)
          .query(q)
          .enableMaterializations(true)
          .explainMatches("", CONTAINS_M0)
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testSingleMaterializationMultiUsage() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";
    String m = "select * from \"emps\" where \"empid\" < 500";
    sql(m, q).withChecker(
        CalciteAssert.checkResultContains(
            "EnumerableTableScan(table=[[hr, m0]])", 2))
        .ok();
  }

  @Test void testMultiMaterializationMultiUsage() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select \"deptno\", count(*) as c from \"emps\" group by \"deptno\") using (\"deptno\")";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select \"deptno\", count(*) as c, sum(\"empid\") as s from \"emps\" group by \"deptno\"",
              "m1", "select * from \"emps\" where \"empid\" < 500")
          .query(q)
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .explainContains("EnumerableTableScan(table=[[hr, m1]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testMaterializationOnJoinQuery() {
    final String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" < 300 ";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 500")
          .query(q)
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Disabled("Creating mv for depts considering all its column throws exception")
  @Test void testMultiMaterializationOnJoinQuery() {
    final String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" < 300 "
        + "and \"depts\".\"deptno\" > 200";
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 500",
              "m1", "select * from \"depts\" where \"deptno\" > 100")
          .query(q)
          .enableMaterializations(true)
          .explainContains("EnumerableTableScan(table=[[hr, m0]])")
          .explainContains("EnumerableTableScan(table=[[hr, m1]])")
          .sameResultWithMaterializationsDisabled();
    }
  }

  @Test void testAggregateMaterializationOnCountDistinctQuery1() {
    // The column empid is already unique, thus DISTINCT is not
    // in the COUNT of the resulting rewriting
    sql("select \"deptno\", \"empid\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"deptno\", count(distinct \"empid\") as c from (\n"
            + "select \"deptno\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\")\n"
            + "group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{0}], C=[COUNT($1)])\n"
                + "  EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  @Test void testAggregateMaterializationOnCountDistinctQuery2() {
    // The column empid is already unique, thus DISTINCT is not
    // in the COUNT of the resulting rewriting
    sql("select \"deptno\", \"salary\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\", \"empid\"",
        "select \"deptno\", count(distinct \"empid\") as c from (\n"
            + "select \"deptno\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\")\n"
            + "group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{0}], C=[COUNT($2)])\n"
                + "  EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  @Test void testAggregateMaterializationOnCountDistinctQuery3() {
    // The column salary is not unique, thus we end up with
    // a different rewriting
    sql("select \"deptno\", \"empid\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"deptno\", count(distinct \"salary\") from (\n"
            + "select \"deptno\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\")\n"
            + "group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT($1)])\n"
                + "  EnumerableAggregate(group=[{0, 2}])\n"
                + "    EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  @Test void testAggregateMaterializationOnCountDistinctQuery4() {
    // Although there is no DISTINCT in the COUNT, this is
    // equivalent to previous query
    sql("select \"deptno\", \"salary\", \"empid\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\", \"empid\"",
        "select \"deptno\", count(\"salary\") from (\n"
            + "select \"deptno\", \"salary\"\n"
            + "from \"emps\"\n"
            + "group by \"deptno\", \"salary\")\n"
            + "group by \"deptno\"")
        .withResultContains(
            "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT()])\n"
                + "  EnumerableAggregate(group=[{0, 1}])\n"
                + "    EnumerableTableScan(table=[[hr, m0]]")
        .ok();
  }

  @Test void testMaterializationSubstitution() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";

    final String[][][] expectedNames = {
        {{"hr", "emps"}, {"hr", "m0"}},
        {{"hr", "emps"}, {"hr", "m1"}},
        {{"hr", "m0"}, {"hr", "emps"}},
        {{"hr", "m0"}, {"hr", "m0"}},
        {{"hr", "m0"}, {"hr", "m1"}},
        {{"hr", "m1"}, {"hr", "emps"}},
        {{"hr", "m1"}, {"hr", "m0"}},
        {{"hr", "m1"}, {"hr", "m1"}}};

    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final List<List<List<String>>> substitutedNames = new ArrayList<>();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 300",
              "m1", "select * from \"emps\" where \"empid\" < 600")
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
      substitutedNames.sort(CASE_INSENSITIVE_LIST_LIST_COMPARATOR);
      assertThat(substitutedNames, is(list3(expectedNames)));
    }
  }

  @Test void testMaterializationSubstitution2() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join (select * from \"emps\" where \"empid\" < 200) using (\"empid\")";

    final String[][][] expectedNames = {
        {{"hr", "emps"}, {"hr", "m0"}},
        {{"hr", "emps"}, {"hr", "m1"}},
        {{"hr", "emps"}, {"hr", "m2"}},
        {{"hr", "m0"}, {"hr", "emps"}},
        {{"hr", "m0"}, {"hr", "m0"}},
        {{"hr", "m0"}, {"hr", "m1"}},
        {{"hr", "m0"}, {"hr", "m2"}},
        {{"hr", "m1"}, {"hr", "emps"}},
        {{"hr", "m1"}, {"hr", "m0"}},
        {{"hr", "m1"}, {"hr", "m1"}},
        {{"hr", "m1"}, {"hr", "m2"}},
        {{"hr", "m2"}, {"hr", "emps"}},
        {{"hr", "m2"}, {"hr", "m0"}},
        {{"hr", "m2"}, {"hr", "m1"}},
        {{"hr", "m2"}, {"hr", "m2"}}};

    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      MaterializationService.setThreadLocal();
      final List<List<List<String>>> substitutedNames = new ArrayList<>();
      CalciteAssert.that()
          .withMaterializations(HR_FKUK_MODEL,
              "m0", "select * from \"emps\" where \"empid\" < 300",
              "m1", "select * from \"emps\" where \"empid\" < 600",
              "m2", "select * from \"m1\"")
          .query(q)
          .withHook(Hook.SUB, (Consumer<RelNode>) r ->
              substitutedNames.add(new TableNameVisitor().run(r)))
          .enableMaterializations(true)
          .sameResultWithMaterializationsDisabled();
      substitutedNames.sort(CASE_INSENSITIVE_LIST_LIST_COMPARATOR);
      assertThat(substitutedNames, is(list3(expectedNames)));
    }
  }

  @Test void testMaterializationAfterTrimingOfUnusedFields() {
    String sql =
        "select \"y\".\"deptno\", \"y\".\"name\", \"x\".\"sum_salary\"\n"
            + "from\n"
            + "  (select \"deptno\", sum(\"salary\") \"sum_salary\"\n"
            + "  from \"emps\"\n"
            + "  group by \"deptno\") \"x\"\n"
            + "  join\n"
            + "  \"depts\" \"y\"\n"
            + "  on \"x\".\"deptno\"=\"y\".\"deptno\"\n";
    sql(sql, sql).ok();
  }

  @Test void testUnionAllToUnionAll() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    sql(sql0 + " union all " + sql1, sql1 + " union all " + sql0).ok();
  }

  @Test void testUnionDistinctToUnionDistinct() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    sql(sql0 + " union " + sql1, sql1 + " union " + sql0).ok();
  }

  @Test void testUnionDistinctToUnionAll() {
    String sql0 = "select * from \"emps\" where \"empid\" < 300";
    String sql1 = "select * from \"emps\" where \"empid\" > 200";
    sql(sql0 + " union " + sql1, sql0 + " union all " + sql1).noMat();
  }

  @Test void testUnionOnCalcsToUnion() {
    final String mv = ""
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300\n"
        + "union all\n"
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100";
    final String query = ""
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300 and \"salary\" > 100\n"
        + "union all\n"
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100 and \"salary\" > 100";
    sql(mv, query).ok();
  }

  @Test void testIntersectOnCalcsToIntersect() {
    final String mv = ""
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300\n"
        + "intersect all\n"
        + "select \"deptno\", \"salary\"\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100";
    final String query = ""
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" > 300 and \"salary\" > 100\n"
        + "intersect all\n"
        + "select \"deptno\", \"salary\" * 2\n"
        + "from \"emps\"\n"
        + "where \"empid\" < 100 and \"salary\" > 100";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testIntersectToIntersect0() {
    final String mv = ""
        + "select \"deptno\" from \"emps\"\n"
        + "intersect\n"
        + "select \"deptno\" from \"depts\"";
    final String query = ""
        + "select \"deptno\" from \"depts\"\n"
        + "intersect\n"
        + "select \"deptno\" from \"emps\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testIntersectToIntersect1() {
    final String mv = ""
        + "select \"deptno\" from \"emps\"\n"
        + "intersect all\n"
        + "select \"deptno\" from \"depts\"";
    final String query = ""
        + "select \"deptno\" from \"depts\"\n"
        + "intersect all\n"
        + "select \"deptno\" from \"emps\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test void testIntersectToCalcOnIntersect() {
    final String intersect = ""
        + "select \"deptno\",\"name\" from \"emps\"\n"
        + "intersect all\n"
        + "select \"deptno\",\"name\" from \"depts\"";
    final String mv = "select \"name\", \"deptno\" from (" + intersect + ")";

    final String query = ""
        + "select \"name\",\"deptno\" from \"depts\"\n"
        + "intersect all\n"
        + "select \"name\",\"deptno\" from \"emps\"";
    sql(mv, query).withOnlyBySubstitution(true).ok();
  }

  @Test public void testMvPredicate1() {
    checkMaterialize(
        "select \"deptno\", \"name\", \"empid\","
            + "count(*) from \"emps\" group by  \"name\", \"deptno\", \"empid\" ",
        "select \"name\", count(*) from \"emps\" where \"deptno\"= 10  group by \"name\" ", true);
  }

  @Test public void testMvPredicate2() {
    checkMaterialize(
        "select \"deptno\", \"name\", \"empid\", count(*) from "
            + "\"emps\" group by  \"name\", \"deptno\", \"empid\" ",
        "select \"name\", count(*) from \"emps\" where "
            + "\"name\"= 'Sebastian' OR \"name\"= 'Peter'  group by \"name\" ", true);
  }

  @Test public void testMvPredicate3() {
    checkMaterialize(
        "select \"deptno\", \"name\", \"empid\", count(*) from "
            + "\"emps\" group by  \"name\", \"deptno\", \"empid\" ",
        "select \"name\", count(*) from \"emps\" where "
            + "\"name\" = 'Sebastian' group by \"name\" ", true);
  }

  @Test public void testMvPredicate4() {
    checkMaterialize(
        "select \"name\", \"deptno\", \"empid\", "
            + "count(*) from \"emps\" group by \"name\", \"deptno\", \"empid\" ",
        "select \"name\", count(*) from \"emps\" where \"name\"= "
            + "'Sebastian' group by \"name\" ", true);
  }

  private static <E> List<List<List<E>>> list3(E[][][] as) {
    final ImmutableList.Builder<List<List<E>>> builder =
        ImmutableList.builder();
    for (E[][] a : as) {
      builder.add(list2(a));
    }
    return builder.build();
  }

  private static <E> List<List<E>> list2(E[][] as) {
    final ImmutableList.Builder<List<E>> builder = ImmutableList.builder();
    for (E[] a : as) {
      builder.add(ImmutableList.copyOf(a));
    }
    return builder.build();
  }

  /**
   * Implementation of RelVisitor to extract substituted table names.
   */
  private static class TableNameVisitor extends RelVisitor {
    private List<List<String>> names = new ArrayList<>();

    List<List<String>> run(RelNode input) {
      go(input);
      return names;
    }

    @Override public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof TableScan) {
        RelOptTable table = node.getTable();
        List<String> qName = table.getQualifiedName();
        names.add(qName);
      }
      super.visit(node, ordinal, parent);
    }
  }

  /**
   * Hr schema with FK-UK relationship.
   */
  public static class HrFKUKSchema {
    @Override public String toString() {
      return "HrFKUKSchema";
    }

    public final Employee[] emps = {
        new Employee(100, 10, "Bill", 10000, 1000),
        new Employee(200, 20, "Eric", 8000, 500),
        new Employee(150, 10, "Sebastian", 7000, null),
        new Employee(110, 10, "Theodore", 10000, 250),
    };
    public final Department[] depts = {
        new Department(10, "Sales", Arrays.asList(emps[0], emps[2], emps[3]),
            new Location(-122, 38)),
        new Department(30, "Marketing", ImmutableList.of(),
            new Location(0, 52)),
        new Department(20, "HR", Collections.singletonList(emps[1]), null),
    };
    public final Dependent[] dependents = {
        new Dependent(10, "Michael"),
        new Dependent(10, "Jane"),
    };
    public final Dependent[] locations = {
        new Dependent(10, "San Francisco"),
        new Dependent(20, "San Diego"),
    };
    public final Event[] events = {
        new Event(100, new Timestamp(0)),
        new Event(200, new Timestamp(0)),
        new Event(150, new Timestamp(0)),
        new Event(110, null),
    };

    public final RelReferentialConstraint rcs0 =
        RelReferentialConstraintImpl.of(
            ImmutableList.of("hr", "emps"), ImmutableList.of("hr", "depts"),
            ImmutableList.of(IntPair.of(1, 0)));

    public QueryableTable foo(int count) {
      return Smalls.generateStrings(count);
    }

    public TranslatableTable view(String s) {
      return Smalls.view(s);
    }

    public TranslatableTable matview() {
      return Smalls.strView("noname");
    }
  }

  /** Fluent class that contains information necessary to run a test. */
  public interface Sql {
    default void ok() {
      that().sameResultWithMaterializationsDisabled();
    }

    default CalciteAssert.AssertQuery that() {
      return checkThatMaterialize_(getMaterialize(), getQuery(), getViewName(),
          isExisting(), getModel(), getChecker(), getRuleSet(),
          isOnlyBySubstitution());
    }

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isExisting();
    Sql withExisting(boolean existing);

    default void noMat() {
      checkNoMaterialize_(getMaterialize(), getQuery(),
          getModel(), isOnlyBySubstitution());
    }

    default Sql withResultContains(String... expected) {
      return withChecker(CalciteAssert.checkResultContains(expected));
    }

    @ImmutableBeans.Property
    String getMaterialize();
    Sql withMaterialize(String materialize);

    @ImmutableBeans.Property
    String getQuery();
    Sql withQuery(String query);

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isOnlyBySubstitution();
    Sql withOnlyBySubstitution(boolean onlyBySubstitution);

    @ImmutableBeans.Property
    String getModel();
    Sql withModel(String model);

    @ImmutableBeans.Property
    Consumer<ResultSet> getChecker();
    Sql withChecker(Consumer<ResultSet> explainChecker);

    @ImmutableBeans.Property
    RuleSet getRuleSet();
    Sql withRuleSet(RuleSet ruleSet);

    @ImmutableBeans.Property
    @ImmutableBeans.StringDefault("m0")
    String getViewName();
    Sql withViewName(String viewName);
  }
}
