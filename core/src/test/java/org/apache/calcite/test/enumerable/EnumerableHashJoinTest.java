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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ReflectiveSchemaWithoutRowCount;
import org.apache.calcite.test.schemata.hr.HrSchema;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

/**
 * Unit test for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableHashJoin}.
 */
class EnumerableHashJoinTest {

  @Test void innerJoin() {
    tester(false, new HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e join depts "
                + "d on e.deptno=d.deptno")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE))
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=Sales");
  }

  @Test void leftOuterJoin() {
    tester(false, new HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e  left outer "
                + "join depts d on e.deptno=d.deptno")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE))
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[left])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=Sales",
            "empid=200; name=Eric; dept=null");
  }

  @Test void rightOuterJoin() {
    tester(false, new HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e  right outer "
                + "join depts d on e.deptno=d.deptno")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[right])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=Sales",
            "empid=null; name=null; dept=Marketing",
            "empid=null; name=null; dept=HR");
  }

  @Test void leftOuterJoinWithPredicate() {
    tester(false, new HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e left outer "
                + "join depts d on e.deptno=d.deptno and e.empid<150 and e"
                + ".empid>d.deptno")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE))
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[AND(=($1, $3), <($0, 150), >"
            + "($0, $3))], joinType=[left])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=150; name=Sebastian; dept=null",
            "empid=200; name=Eric; dept=null");
  }

  @Test void rightOuterJoinWithPredicate() {
    tester(false, new HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e right outer "
                + "join depts d on e.deptno=d.deptno and e.empid<150")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], "
            + "name=[$t2], dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[right])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[150], "
            + "expr#6=[<($t0, $t5)], proj#0..2=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales",
            "empid=null; name=null; dept=Marketing",
            "empid=null; name=null; dept=HR");
  }


  @Test void semiJoin() {
    tester(false, new HrSchema())
        .query(
            "SELECT d.deptno, d.name FROM depts d WHERE d.deptno in (SELECT e.deptno FROM emps e)")
        .explainContains("EnumerableHashJoin(condition=[=($0, $3)], "
            + "joinType=[semi])\n"
            + "  EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "    EnumerableTableScan(table=[[s, depts]])\n"
            + "  EnumerableTableScan(table=[[s, emps]])")
        .returnsUnordered(
            "deptno=10; name=Sales");
  }

  @Test void semiJoinWithPredicate() {
    tester(false, new HrSchema())
        .withRel(
            // Retrieve employees with the top salary in their department. Equivalent SQL:
            //   SELECT e.name, e.salary FROM emps e
            //   WHERE  EXISTS (
            //     SELECT 1 FROM emps e2
            //     WHERE e.deptno = e2.deptno AND e2.salary > e.salary)
            builder -> builder
                .scan("s", "emps").as("e")
                .scan("s", "emps").as("e2")
                .semiJoin(
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
            "name=Bill; salary=10000.0",
            "name=Sebastian; salary=7000.0");
  }

  @Test void innerJoinWithPredicate() {
    tester(false, new HrSchema())
        .query(
            "select e.empid, e.name, d.name as dept from emps e join depts d"
                + " on e.deptno=d.deptno and e.empid<150 and e.empid>d.deptno")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2], "
            + "dept=[$t4])\n"
            + "  EnumerableHashJoin(condition=[AND(=($1, $3), >($0, $3))], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[150], expr#6=[<($t0, $t5)], "
            + "proj#0..2=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[s, depts]])\n")
        .returnsUnordered(
            "empid=100; name=Bill; dept=Sales",
            "empid=110; name=Theodore; dept=Sales");
  }

  @Test void innerJoinWithCompositeKeyAndNullValues() {
    tester(false, new HrSchema())
        .query(
            "select e1.empid from emps e1 join emps e2 "
                + "on e1.deptno=e2.deptno and e1.commission=e2.commission")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE))
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0])\n"
            + "  EnumerableHashJoin(condition=[AND(=($1, $3), =($2, $4))], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], deptno=[$t1], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsUnordered(
            "empid=100",
            "empid=110",
            "empid=200");
  }

  @Test void leftOuterJoinWithCompositeKeyAndNullValues() {
    tester(false, new HrSchema())
        .query(
            "select e1.empid, e2.empid from emps e1 left outer join emps e2 "
                + "on e1.deptno=e2.deptno and e1.commission=e2.commission")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE))
        .explainContains("EnumerableCalc(expr#0..5=[{inputs}], empid=[$t0], empid0=[$t3])\n"
            + "  EnumerableHashJoin(condition=[AND(=($1, $4), =($2, $5))], joinType=[left])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsUnordered(
            "empid=100; empid=100",
            "empid=110; empid=110",
            "empid=150; empid=null",
            "empid=200; empid=200");
  }

  @Test void rightOuterJoinWithCompositeKeyAndNullValues() {
    tester(false, new HrSchema())
        .query(
            "select e1.empid, e2.empid from emps e1 right outer join emps e2 "
                + "on e1.deptno=e2.deptno and e1.commission=e2.commission")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE))
        .explainContains("EnumerableCalc(expr#0..5=[{inputs}], empid=[$t0], empid0=[$t3])\n"
            + "  EnumerableHashJoin(condition=[AND(=($1, $4), =($2, $5))], joinType=[right])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsUnordered(
            "empid=100; empid=100",
            "empid=110; empid=110",
            "empid=200; empid=200",
            "empid=null; empid=150");
  }

  @Test void fullOuterJoinWithCompositeKeyAndNullValues() {
    tester(false, new HrSchema())
        .query(
            "select e1.empid, e2.empid from emps e1 full outer join emps e2 "
                + "on e1.deptno=e2.deptno and e1.commission=e2.commission")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE))
        .explainContains("EnumerableCalc(expr#0..5=[{inputs}], empid=[$t0], empid0=[$t3])\n"
            + "  EnumerableHashJoin(condition=[AND(=($1, $4), =($2, $5))], joinType=[full])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsUnordered(
            "empid=100; empid=100",
            "empid=110; empid=110",
            "empid=150; empid=null",
            "empid=200; empid=200",
            "empid=null; empid=150");
  }

  @Test void semiJoinWithCompositeKeyAndNullValues() {
    tester(true, new HrSchema())
        .query(
            "select e1.empid from emps e1 where exists (select 1 from emps e2 "
                + "where e1.deptno=e2.deptno and e1.commission=e2.commission)")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        })
        .explainContains("EnumerableCalc(expr#0..2=[{inputs}], empid=[$t0])\n"
            + "  EnumerableHashJoin(condition=[AND(=($1, $4), =($2, $7))], joinType=[semi])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}], commission=[$t4])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[IS NOT NULL($t4)], proj#0..4=[{exprs}], $condition=[$t5])\n"
            + "      EnumerableTableScan(table=[[s, emps]])\n")
        .returnsUnordered(
            "empid=100",
            "empid=110",
            "empid=200");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
      Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchemaWithoutRowCount(schema));
  }
}
