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
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.JoinToCorrelateRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcTest;

import org.junit.Test;

import java.util.function.Consumer;

/**
 * Unit test for
 * {@link EnumerableCorrelate}.
 */
public class EnumerableCorrelateTest {
  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2605">[CALCITE-2605]
   * NullPointerException when left outer join implemented with EnumerableCorrelate</a> */
  @Test public void leftOuterJoinCorrelate() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e left outer join depts d on e.deptno=d.deptno")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the left outer join to run via EnumerableCorrelate
          // instead of EnumerableHashJoin
          planner.addRule(JoinToCorrelateRule.INSTANCE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
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

  @Test public void simpleCorrelateDecorrelated() {
    tester(true, new JdbcTest.HrSchema())
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
   * Add rule to execute semi joins with correlation</a> */
  @Test public void semiJoinCorrelate() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select empid, name from emps e where e.deptno in (select d.deptno from depts d)")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the semijoin to run via EnumerableCorrelate
          // instead of EnumerableHashJoin(SEMI)
          planner.addRule(JoinToCorrelateRule.INSTANCE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .explainContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], empid=[$t1], name=[$t3])\n"
            + "  EnumerableCorrelate(correlation=[$cor2], joinType=[inner], requiredColumns=[{0}])\n"
            + "    EnumerableAggregate(group=[{0}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[$cor2], expr#6=[$t5.deptno], expr#7=[=($t1, $t6)], proj#0..2=[{exprs}], $condition=[$t7])\n"
            + "      EnumerableTableScan(table=[[s, emps]])")
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2930">[CALCITE-2930]
   * FilterCorrelateRule on a Correlate with SemiJoinType SEMI (or ANTI)
   * throws IllegalStateException</a> */
  @Test public void semiJoinCorrelateWithFilterCorrelateRule() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select empid, name from emps e where e.deptno in (select d.deptno from depts d) and e.empid > 100")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the semijoin to run via EnumerableCorrelate
          // instead of EnumerableHashJoin(SEMI),
          // and push the 'empid > 100' filter into the Correlate
          planner.addRule(JoinToCorrelateRule.INSTANCE);
          planner.addRule(FilterCorrelateRule.INSTANCE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .explainContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], empid=[$t1], name=[$t3])\n"
            + "  EnumerableCorrelate(correlation=[$cor5], joinType=[inner], requiredColumns=[{0}])\n"
            + "    EnumerableAggregate(group=[{0}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[100], expr#6=[>($t0, $t5)], expr#7=[$cor5], expr#8=[$t7.deptno], expr#9=[=($t1, $t8)], expr#10=[AND($t6, $t9)], proj#0..2=[{exprs}], $condition=[$t10])\n"
            + "      EnumerableTableScan(table=[[s, emps]])")
        .returnsUnordered(
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");
  }

  @Test public void simpleCorrelate() {
    tester(false, new JdbcTest.HrSchema())
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

  @Test public void simpleCorrelateWithConditionIncludingBoxedPrimitive() {
    final String sql = "select empid from emps e where not exists (\n"
        + "  select 1 from depts d where d.deptno=e.commission)";
    tester(false, new JdbcTest.HrSchema())
        .query(sql)
        .returnsUnordered(
            "empid=100",
            "empid=110",
            "empid=150",
            "empid=200");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2920">[CALCITE-2920]
   * RelBuilder: new method to create an anti-join</a>. */
  @Test public void antiJoinCorrelate() {
    tester(false, new JdbcTest.HrSchema())
        .query("?")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the antijoin to run via EnumerableCorrelate
          // instead of EnumerableHashJoin(ANTI)
          planner.addRule(JoinToCorrelateRule.INSTANCE);
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

  @Test public void nonEquiAntiJoinCorrelate() {
    tester(false, new JdbcTest.HrSchema())
        .query("?")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the antijoin to run via EnumerableCorrelate
          // instead of EnumerableNestedLoopJoin
          planner.addRule(JoinToCorrelateRule.INSTANCE);
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
   * RelBuilder: new method to create an antijoin</a> */
  @Test public void antiJoinCorrelateWithNullValues() {
    final Integer salesDeptNo = 10;
    tester(false, new JdbcTest.HrSchema())
        .query("?")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          // force the antijoin to run via EnumerableCorrelate
          // instead of EnumerableHashJoin(ANTI)
          planner.addRule(JoinToCorrelateRule.INSTANCE);
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

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}

// End EnumerableCorrelateTest.java
