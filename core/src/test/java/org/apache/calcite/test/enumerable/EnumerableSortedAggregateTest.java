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
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcTest;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

public class EnumerableSortedAggregateTest {
  @Test void sortedAgg() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select deptno, "
            + "max(salary) as max_salary, count(name) as num_employee "
            + "from emps group by deptno")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);
        })
        .explainContains(
            "EnumerableSortedAggregate(group=[{1}], max_salary=[MAX($3)], num_employee=[COUNT($2)])\n"
            + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "deptno=10; max_salary=11500.0; num_employee=3",
            "deptno=20; max_salary=8000.0; num_employee=1");
  }

  @Test void sortedAggTwoGroupKeys() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select deptno, commission, "
                + "max(salary) as max_salary, count(name) as num_employee "
                + "from emps group by deptno, commission")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);
        })
        .explainContains(
            "EnumerableSortedAggregate(group=[{1, 4}], max_salary=[MAX($3)], num_employee=[COUNT($2)])\n"
            + "  EnumerableSort(sort0=[$1], sort1=[$4], dir0=[ASC], dir1=[ASC])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "deptno=10; commission=250; max_salary=11500.0; num_employee=1",
            "deptno=10; commission=1000; max_salary=10000.0; num_employee=1",
            "deptno=10; commission=null; max_salary=7000.0; num_employee=1",
            "deptno=20; commission=500; max_salary=8000.0; num_employee=1");
  }

  // Outer sort is expected to be pushed through aggregation.
  @Test void sortedAggGroupbyXOrderbyX() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select deptno, "
                + "max(salary) as max_salary, count(name) as num_employee "
                + "from emps group by deptno order by deptno")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);
        })
        .explainContains(
            "EnumerableSortedAggregate(group=[{1}], max_salary=[MAX($3)], num_employee=[COUNT($2)])\n"
            + "  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "deptno=10; max_salary=11500.0; num_employee=3",
            "deptno=20; max_salary=8000.0; num_employee=1");
  }

  // Outer sort is not expected to be pushed through aggregation.
  @Test void sortedAggGroupbyXOrderbyY() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select deptno, "
                + "max(salary) as max_salary, count(name) as num_employee "
                + "from emps group by deptno order by num_employee desc")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);
        })
        .explainContains(
            "EnumerableSort(sort0=[$2], dir0=[DESC])\n"
            + "  EnumerableSortedAggregate(group=[{1}], max_salary=[MAX($3)], num_employee=[COUNT($2)])\n"
            + "    EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "      EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "deptno=10; max_salary=11500.0; num_employee=3",
            "deptno=20; max_salary=8000.0; num_employee=1");
  }

  @Test void sortedAggNullValueInSortedGroupByKeys() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select commission, "
                + "count(deptno) as num_dept "
                + "from emps group by commission")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);
        })
        .explainContains(
            "EnumerableSortedAggregate(group=[{4}], num_dept=[COUNT()])\n"
                + "  EnumerableSort(sort0=[$4], dir0=[ASC])\n"
                + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=250; num_dept=1",
            "commission=500; num_dept=1",
            "commission=1000; num_dept=1",
            "commission=null; num_dept=1");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
                                          Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}
