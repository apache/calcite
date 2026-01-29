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
package org.apache.calcite.test.enumerable;

import org.apache.calcite.adapter.enumerable.EnumerableCorrelate;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ReflectiveSchemaWithoutRowCount;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

/**
 * Unit test for
 * {@link EnumerableCorrelate}.
 */
class EnumerableCorrelateTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2605">[CALCITE-2605]
   * NullPointerException when left outer join implemented with
   * EnumerableCorrelate</a>. */
  @Test void leftOuterJoinCorrelate() {
    tester(false, new HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e left outer join depts d on e.deptno=d.deptno")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the left outer join to run via EnumerableCorrelate
          // instead of EnumerableHashJoin
          planner.addRule(CoreRules.JOIN_TO_CORRELATE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
          planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        })
        .explainContains(""
            + "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2], dept=[$t4])\n"
            + "  EnumerableCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[$cor0], expr#5=[$t4.deptno], expr#6=[=($t5, $t0)], proj#0..1=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=Sales",
            "empid=200; name=Eric; dept=null");
  }

  @Test void simpleCorrelateDecorrelated() {
    tester(true, new HrSchema())
        .query(
            "select empid, name from emps e where exists (select 1 from depts d where d.deptno=e.deptno)")
        .explainContains(""
            + "EnumerableCalc(expr#0..2=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[semi])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2621">[CALCITE-2621]
   * Add rule to execute semi joins with correlation</a>. */
  @Test void semiJoinCorrelate() {
    tester(false, new HrSchema())
        .query(
            "select empid, name from emps e where e.deptno in (select d.deptno from depts d)")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the semijoin to run via EnumerableCorrelate
          // instead of EnumerableHashJoin(SEMI)
          planner.addRule(CoreRules.JOIN_TO_CORRELATE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
          planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        })
        .explainContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], empid=[$t1], name=[$t3])\n"
            + "  EnumerableCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
            + "    EnumerableAggregate(group=[{0}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[$cor1], expr#6=[$t5.deptno], expr#7=[=($t1, $t6)], proj#0..2=[{exprs}], $condition=[$t7])\n"
            + "      EnumerableTableScan(table=[[s, emps]])")
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2930">[CALCITE-2930]
   * FilterCorrelateRule on a Correlate with SemiJoinType SEMI (or ANTI) throws
   * IllegalStateException</a>. */
  @Test void semiJoinCorrelateWithFilterCorrelateRule() {
    tester(false, new HrSchema())
        .query(
            "select empid, name from emps e where e.deptno in (select d.deptno from depts d) and e.empid > 100")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the semijoin to run via EnumerableCorrelate
          // instead of EnumerableHashJoin(SEMI),
          // and push the 'empid > 100' filter into the Correlate
          planner.addRule(CoreRules.JOIN_TO_CORRELATE);
          planner.addRule(CoreRules.FILTER_CORRELATE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
          planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        })
        .explainContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], empid=[$t1], name=[$t3])\n"
            + "  EnumerableCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
            + "    EnumerableAggregate(group=[{0}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[$cor1], expr#6=[$t5.deptno], expr#7=[=($t1, $t6)], expr#8=[CAST($t0):INTEGER NOT NULL], expr#9=[100], expr#10=[>($t8, $t9)], expr#11=[AND($t7, $t10)], proj#0..2=[{exprs}], $condition=[$t11])\n"
            + "      EnumerableTableScan(table=[[s, emps]])")
        .returnsUnordered(
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }

  @Test void simpleCorrelate() {
    tester(false, new HrSchema())
        .query(
            "select empid, name from emps e where exists (select 1 from depts d where d.deptno=e.deptno)")
        .explainContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "  EnumerableCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableAggregate(group=[{0}])\n"
            + "      EnumerableCalc(expr#0..3=[{inputs}], expr#4=[true], expr#5=[$cor0], expr#6=[$t5.deptno], expr#7=[=($t0, $t6)], i=[$t4], $condition=[$t7])\n"
            + "        EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }

  @Test void simpleCorrelateWithConditionIncludingBoxedPrimitive() {
    final String sql = "select empid from emps e where not exists (\n"
        + "  select 1 from depts d where d.deptno=e.commission)";
    tester(false, new HrSchema())
        .query(sql)
        .returnsUnordered(
            "empid=100",
            "empid=110",
            "empid=150",
            "empid=200");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5638">[CALCITE-5638]
   * Columns trimmer need to consider sub queries</a>.
   */
  @Test void complexNestedCorrelatedSubquery() {
    String sql = "SELECT empid, deptno, (SELECT count(*) FROM emps AS x "
        + "WHERE x.salary>emps.salary and x.deptno<emps.deptno) FROM emps "
        + "WHERE empid<salary ORDER BY 1,2,3";

    tester(false, new HrSchema())
        .query(sql)
        .returnsOrdered(
            "empid=100; deptno=10; EXPR$2=0",
            "empid=110; deptno=10; EXPR$2=0",
            "empid=150; deptno=10; EXPR$2=0",
            "empid=200; deptno=20; EXPR$2=2");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2920">[CALCITE-2920]
   * RelBuilder: new method to create an anti-join</a>. */
  @Test void antiJoinCorrelate() {
    tester(false, new HrSchema())
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the antijoin to run via EnumerableCorrelate
          // instead of EnumerableHashJoin(ANTI)
          planner.addRule(CoreRules.JOIN_TO_CORRELATE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .withRel(
            // Retrieve departments without employees. Equivalent SQL:
            //   SELECT d.deptno, d.name FROM depts d
            //   WHERE NOT EXISTS (SELECT 1 FROM emps e WHERE e.deptno = d.deptno)
            builder -> builder
                .scan("s", "depts").as("d")
                .scan("s", "emps").as("e")
                .antiJoin(
                    builder.equals(
                        builder.field(2, "d", "deptno"),
                        builder.field(2, "e", "deptno")))
                .project(
                    builder.field("deptno"),
                    builder.field("name"))
                .build())
        .returnsUnordered(
            "deptno=30; name=Marketing",
            "deptno=40; name=HR");
  }

  @Test void nonEquiAntiJoinCorrelate() {
    tester(false, new HrSchema())
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the antijoin to run via EnumerableCorrelate
          // instead of EnumerableNestedLoopJoin
          planner.addRule(CoreRules.JOIN_TO_CORRELATE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .withRel(
            // Retrieve employees with the top salary in their department. Equivalent SQL:
            //   SELECT e.name, e.salary FROM emps e
            //   WHERE NOT EXISTS (
            //     SELECT 1 FROM emps e2
            //     WHERE e.deptno = e2.deptno AND e2.salary > e.salary)
            builder -> builder
                .scan("s", "emps").as("e")
                .scan("s", "emps").as("e2")
                .antiJoin(
                    builder.and(
                        builder.equals(
                            builder.field(2, "e", "deptno"),
                            builder.field(2, "e2", "deptno")),
                        builder.call(
                            SqlStdOperatorTable.GREATER_THAN,
                            builder.field(2, "e2", "salary"),
                            builder.field(2, "e", "salary"))))
                .project(
                    builder.field("name"),
                    builder.field("salary"))
                .build())
        .returnsUnordered(
            "name=Theodore; salary=11500.0",
            "name=Eric; salary=8000.0");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2920">[CALCITE-2920]
   * RelBuilder: new method to create an antijoin</a>. */
  @Test void antiJoinCorrelateWithNullValues() {
    final Integer salesDeptNo = 10;
    tester(false, new HrSchema())
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the antijoin to run via EnumerableCorrelate
          // instead of EnumerableHashJoin(ANTI)
          planner.addRule(CoreRules.JOIN_TO_CORRELATE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .withRel(
            // Retrieve employees from any department other than Sales (deptno 10) whose
            // commission is different from any Sales employee commission. Since there
            // is a Sales employee with null commission, the goal is to validate that antiJoin
            // behaves as a NOT EXISTS (and returns results), and not as a NOT IN (which would
            // not return any result due to its null handling). Equivalent SQL:
            //   SELECT empOther.empid, empOther.name FROM emps empOther
            //   WHERE empOther.deptno <> 10 AND NOT EXISTS
            //     (SELECT 1 FROM emps empSales
            //      WHERE empSales.deptno = 10 AND empSales.commission = empOther.commission)
            builder -> builder
                .scan("s", "emps").as("empOther")
                .filter(
                    builder.notEquals(
                        builder.field("empOther", "deptno"),
                        builder.literal(salesDeptNo)))
                .scan("s", "emps").as("empSales")
                .filter(
                    builder.equals(
                        builder.field("empSales", "deptno"),
                        builder.literal(salesDeptNo)))
                .antiJoin(
                    builder.equals(
                        builder.field(2, "empOther", "commission"),
                        builder.field(2, "empSales", "commission")))
                .project(
                    builder.field("empid"),
                    builder.field("name"))
                .build())
        .returnsUnordered("empid=200; name=Eric");
  }

  private static Program getConditionalCorrelateProgram() {
    Program subQuery =
        Programs.hep(
            ImmutableList.of(CoreRules.PROJECT_SUB_QUERY_TO_MARK_CORRELATE,
                CoreRules.FILTER_SUB_QUERY_TO_MARK_CORRELATE),
            true,
            DefaultRelMetadataProvider.INSTANCE);
    Program toCalc =
        Programs.hep(
            ImmutableList.of(
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_TO_CALC,
                CoreRules.CALC_MERGE),
            true,
            DefaultRelMetadataProvider.INSTANCE);

    final List<RelOptRule> enumerableRules =
        ImmutableList.of(
            EnumerableRules.ENUMERABLE_VALUES_RULE,
            EnumerableRules.ENUMERABLE_CALC_RULE,
            EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
            EnumerableRules.ENUMERABLE_CONDITIONAL_CORRELATE_RULE);
    Program enumerableImpl = Programs.ofRules(enumerableRules);
    return Programs.sequence(subQuery, toCalc, enumerableImpl);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7403">[CALCITE-7403]
   * Missing ENUMERABLE Convention for LogicalConditionalCorrelate</a>. */
  @Test void testConditionalCorrelateForExists() {
    // test for exists
    tester(false, new HrSchema())
        .query(
            "WITH t1(id, val) AS (\n"
                + "  VALUES (1, 10), (2, 20), (NULL, 30)\n"
                + "),\n"
                + "t2(id, val) AS (\n"
                + "  VALUES (2, 15), (3, 25)\n"
                + ")\n"
                + "SELECT\n"
                + "  t1.id,\n"
                + "  EXISTS (\n"
                + "    SELECT 1\n"
                + "    FROM t2\n"
                + "    WHERE t2.id = t1.id\n"
                + "      AND t2.val > 10\n"
                + "  ) AS marker\n"
                + "FROM t1")
        .withHook(Hook.PROGRAM, (Consumer<Holder<Program>>) program -> {
          program.set(getConditionalCorrelateProgram());
        })
        .explainHookMatches(""
            + "EnumerableCalc(expr#0..2=[{inputs}], id=[$t0], marker=[$t2])\n"
            + "  EnumerableConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], requiredColumns=[{0}])\n"
            + "    EnumerableValues(tuples=[[{ 1, 10 }, { 2, 20 }, { null, 30 }]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[$cor0], expr#3=[$t2.id], expr#4=[=($t0, $t3)], expr#5=[10], expr#6=[>($t1, $t5)], expr#7=[AND($t4, $t6)], proj#0..1=[{exprs}], $condition=[$t7])\n"
            + "      EnumerableValues(tuples=[[{ 2, 15 }, { 3, 25 }]])\n")
        .returnsUnordered(
            "id=1; marker=false",
            "id=2; marker=true",
            "id=null; marker=false");

    // test for not exists
    tester(false, new HrSchema())
        .query(
            "WITH t1(id, val) AS (\n"
                + "  VALUES (1, 10), (2, 20), (NULL, 30)\n"
                + "),\n"
                + "t2(id, val) AS (\n"
                + "  VALUES (2, 15), (3, 25)\n"
                + ")\n"
                + "SELECT\n"
                + "  t1.id,\n"
                + "  NOT EXISTS (\n"
                + "    SELECT 1\n"
                + "    FROM t2\n"
                + "    WHERE t2.id = t1.id\n"
                + "      AND t2.val > 10\n"
                + "  ) AS marker\n"
                + "FROM t1")
        .withHook(Hook.PROGRAM, (Consumer<Holder<Program>>) program -> {
          program.set(getConditionalCorrelateProgram());
        })
        .explainHookMatches(""
            + "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[NOT($t2)], id=[$t0], marker=[$t3])\n"
            + "  EnumerableConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], requiredColumns=[{0}])\n"
            + "    EnumerableValues(tuples=[[{ 1, 10 }, { 2, 20 }, { null, 30 }]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[$cor0], expr#3=[$t2.id], expr#4=[=($t0, $t3)], expr#5=[10], expr#6=[>($t1, $t5)], expr#7=[AND($t4, $t6)], proj#0..1=[{exprs}], $condition=[$t7])\n"
            + "      EnumerableValues(tuples=[[{ 2, 15 }, { 3, 25 }]])\n")
        .returnsUnordered(
            "id=1; marker=true",
            "id=2; marker=false",
            "id=null; marker=true");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7403">[CALCITE-7403]
   * Missing ENUMERABLE Convention for LogicalConditionalCorrelate</a>. */
  @Test void testConditionalCorrelateForIn() {
    // test in
    tester(false, new HrSchema())
        .query(
            "WITH t1(id, val) AS (\n"
                + "  VALUES (1, 10), (2, 20), (NULL, 30)\n"
                + "),\n"
                + "t2(id, val) AS (\n"
                + "  VALUES (2, 15), (3, 25)\n"
                + ")\n"
                + "SELECT\n"
                + "  t1.id,\n"
                + "  t1.id IN (\n"
                + "    SELECT t2.id\n"
                + "    FROM t2\n"
                + "    WHERE t2.id = t1.id\n"
                + "      AND t2.val > 10\n"
                + "  ) AS marker\n"
                + "FROM t1")
        .withHook(Hook.PROGRAM, (Consumer<Holder<Program>>) program -> {
          program.set(getConditionalCorrelateProgram());
        })
        .explainHookMatches(""
            + "EnumerableCalc(expr#0..2=[{inputs}], id=[$t0], marker=[$t2])\n"
            + "  EnumerableConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], requiredColumns=[{0}], condition=[=($0, $2)])\n"
            + "    EnumerableValues(tuples=[[{ 1, 10 }, { 2, 20 }, { null, 30 }]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[$cor0], expr#3=[$t2.id], expr#4=[=($t0, $t3)], expr#5=[10], expr#6=[>($t1, $t5)], expr#7=[AND($t4, $t6)], id=[$t0], $condition=[$t7])\n"
            + "      EnumerableValues(tuples=[[{ 2, 15 }, { 3, 25 }]])\n")
        .returnsUnordered(
            "id=1; marker=false",
            "id=2; marker=true",
            "id=null; marker=false");

    // test not in
    tester(false, new HrSchema())
        .query(
            "WITH t1(id, val) AS (\n"
                + "  VALUES (1, 10), (2, 20), (NULL, 30)\n"
                + "),\n"
                + "t2(id, val) AS (\n"
                + "  VALUES (2, 15), (3, 25)\n"
                + ")\n"
                + "SELECT\n"
                + "  t1.id,\n"
                + "  t1.id NOT IN (\n"
                + "    SELECT t2.id\n"
                + "    FROM t2\n"
                + "    WHERE t2.id = t1.id\n"
                + "      AND t2.val > 10\n"
                + "  ) AS marker\n"
                + "FROM t1")
        .withHook(Hook.PROGRAM, (Consumer<Holder<Program>>) program -> {
          program.set(getConditionalCorrelateProgram());
        })
        .explainHookMatches(""
            + "EnumerableCalc(expr#0..2=[{inputs}], expr#3=[NOT($t2)], id=[$t0], marker=[$t3])\n"
            + "  EnumerableConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], requiredColumns=[{0}], condition=[=($0, $2)])\n"
            + "    EnumerableValues(tuples=[[{ 1, 10 }, { 2, 20 }, { null, 30 }]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[$cor0], expr#3=[$t2.id], expr#4=[=($t0, $t3)], expr#5=[10], expr#6=[>($t1, $t5)], expr#7=[AND($t4, $t6)], id=[$t0], $condition=[$t7])\n"
            + "      EnumerableValues(tuples=[[{ 2, 15 }, { 3, 25 }]])\n")
        .returnsUnordered(
            "id=1; marker=true",
            "id=2; marker=false",
            "id=null; marker=true");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchemaWithoutRowCount(schema));
  }
}
