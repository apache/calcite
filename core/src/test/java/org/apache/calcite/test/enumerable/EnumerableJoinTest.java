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

import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.schemata.hr.HierarchySchema;
import org.apache.calcite.test.schemata.hr.HrSchema;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

/**
 * Unit tests for the different Enumerable Join implementations.
 */
class EnumerableJoinTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2968">[CALCITE-2968]
   * New AntiJoin relational expression</a>. */
  @Test void equiAntiJoin() {
    tester(false, new HrSchema())
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2968">[CALCITE-2968]
   * New AntiJoin relational expression</a>. */
  @Test void nonEquiAntiJoin() {
    tester(false, new HrSchema())
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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2968">[CALCITE-2968]
   * New AntiJoin relational expression</a>. */
  @Test void equiAntiJoinWithNullValues() {
    final Integer salesDeptNo = 10;
    tester(false, new HrSchema())
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3170">[CALCITE-3170]
   * ANTI join on conditions push down generates wrong plan</a>. */
  @Test void testCanNotPushAntiJoinConditionsToLeft() {
    tester(false, new HrSchema())
        .withRel(
            // build a rel equivalent to sql:
            // select * from emps
            // where emps.deptno
            // not in (select depts.deptno from depts where emps.name = 'ddd')

            // Use `equals` instead of `is not distinct from` only for testing.
            builder -> builder
                .scan("s", "emps")
                .scan("s", "depts")
                .antiJoin(
                    builder.equals(
                        builder.field(2, 0, "deptno"),
                        builder.field(2, 1, "deptno")),
                    builder.equals(builder.field(2, 0, "name"),
                        builder.literal("ddd")))
                .project(builder.field(0))
                .build()
    ).returnsUnordered(
        "empid=100",
        "empid=110",
        "empid=150",
        "empid=200");
  }

  /**
   * The test verifies if {@link EnumerableMergeJoin} can implement a join with non-equi conditions.
   */
  @Test void testSortMergeJoinWithNonEquiCondition() {
    tester(false, new HrSchema())
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .withRel(builder -> builder
            // build a rel equivalent to sql:
            // select e.empid, e.name, d.name as dept, e.deptno, d.deptno
            // from emps e join depts d
            // on e.deptno=d.deptno and e.empid > d.deptno * 10
            // Note: explicit sort is used so EnumerableMergeJoin could actually work
            .scan("s", "emps")
            .sort(builder.field("deptno"))
            .scan("s", "depts")
            .sort(builder.field("deptno"))
            .join(JoinRelType.INNER,
                builder.and(
                    builder.equals(
                        builder.field(2, 0, "deptno"),
                        builder.field(2, 1, "deptno")),
                    builder.getRexBuilder().makeCall(
                        SqlStdOperatorTable.GREATER_THAN,
                        builder.field(2, 0, "empid"),
                        builder.getRexBuilder().makeCall(
                            SqlStdOperatorTable.MULTIPLY,
                            builder.literal(10),
                            builder.field(2, 1, "deptno")))))
            .project(
                builder.field(1, "emps", "empid"),
                builder.field(1, "emps", "name"),
                builder.alias(builder.field(1, "depts", "name"), "dept_name"),
                builder.alias(builder.field(1, "emps", "deptno"), "e_deptno"),
                builder.alias(builder.field(1, "depts", "deptno"), "d_deptno"))
            .build())
        .explainHookMatches("" // It is important that we have MergeJoin in the plan
            + "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2], dept_name=[$t4], e_deptno=[$t1], d_deptno=[$t3])\n"
            + "  EnumerableMergeJoin(condition=[AND(=($1, $3), >($0, *(10, $3)))], joinType=[inner])\n"
            + "    EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "        EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "      EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "        EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(""
            + "empid=110; name=Theodore; dept_name=Sales; e_deptno=10; d_deptno=10\n"
            + "empid=150; name=Sebastian; dept_name=Sales; e_deptno=10; d_deptno=10");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3846">[CALCITE-3846]
   * EnumerableMergeJoin: wrong comparison of composite key with null values</a>. */
  @Test void testMergeJoinWithCompositeKeyAndNullValues() {
    tester(false, new HrSchema())
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .withRel(builder -> builder
            .scan("s", "emps")
            .sort(builder.field("deptno"), builder.field("commission"))
            .scan("s", "emps")
            .sort(builder.field("deptno"), builder.field("commission"))
            .join(JoinRelType.INNER,
                builder.and(
                    builder.equals(
                        builder.field(2, 0, "deptno"),
                        builder.field(2, 1, "deptno")),
                    builder.equals(
                        builder.field(2, 0, "commission"),
                        builder.field(2, 1, "commission"))))
            .project(
                builder.field("empid"))
            .build())
        .explainHookMatches("" // It is important that we have MergeJoin in the plan
            + "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0])\n"
            + "  EnumerableMergeJoin(condition=[AND(=($1, $3), =($2, $4))], joinType=[inner])\n"
            + "    EnumerableSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "        EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], deptno=[$t1], commission=[$t4])\n"
            + "        EnumerableTableScan(table=[[s, emps]])\n")
        .returnsUnordered("empid=100\nempid=110\nempid=150\nempid=200");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3820">[CALCITE-3820]
   * EnumerableDefaults#orderBy should be lazily computed + support enumerator
   * re-initialization</a>. */
  @Test void testRepeatUnionWithMergeJoin() {
    tester(false, new HierarchySchema())
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        // Note: explicit sort is used so EnumerableMergeJoin can actually work
        .withRel(builder -> builder
            //   WITH RECURSIVE delta(empid, name) as (
            //     SELECT empid, name FROM emps WHERE empid = 2
            //     UNION ALL
            //     SELECT e.empid, e.name FROM delta d
            //                            JOIN hierarchies h ON d.empid = h.managerid
            //                            JOIN emps e        ON h.subordinateid = e.empid
            //   )
            //   SELECT empid, name FROM delta
            .scan("s", "emps")
            .filter(
                builder.equals(
                    builder.field("empid"),
                    builder.literal(2)))
            .project(
                builder.field("emps", "empid"),
                builder.field("emps", "name"))

            .transientScan("#DELTA#")
            .sort(builder.field("empid"))
            .scan("s", "hierarchies")
            .sort(builder.field("managerid"))
            .join(
                JoinRelType.INNER,
                builder.equals(
                    builder.field(2, "#DELTA#", "empid"),
                    builder.field(2, "hierarchies", "managerid")))
            .sort(builder.field("subordinateid"))

            .scan("s", "emps")
            .sort(builder.field("empid"))
            .join(
                JoinRelType.INNER,
                builder.equals(
                    builder.field(2, "hierarchies", "subordinateid"),
                    builder.field(2, "emps", "empid")))
            .project(
                builder.field("emps", "empid"),
                builder.field("emps", "name"))
            .repeatUnion("#DELTA#", true)
            .build()
        )
        .explainHookMatches("" // It is important to have MergeJoin + EnumerableSort in the plan
            + "EnumerableRepeatUnion(all=[true])\n"
            + "  EnumerableTableSpool(readType=[LAZY], writeType=[LAZY], table=[[#DELTA#]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[2], expr#6=[=($t0, $t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "  EnumerableTableSpool(readType=[LAZY], writeType=[LAZY], table=[[#DELTA#]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], empid=[$t3], name=[$t4])\n"
            + "      EnumerableMergeJoin(condition=[=($2, $3)], joinType=[inner])\n"
            + "        EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "          EnumerableMergeJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "            EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "              EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
            + "                EnumerableInterpreter\n"
            + "                  BindableTableScan(table=[[#DELTA#]])\n"
            + "            EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "              EnumerableTableScan(table=[[s, hierarchies]])\n"
            + "        EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "          EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "            EnumerableTableScan(table=[[s, emps]])\n")
        .returnsUnordered(""
            + "empid=2; name=Emp2\n"
            + "empid=3; name=Emp3\n"
            + "empid=5; name=Emp5");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}
