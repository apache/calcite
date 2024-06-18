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
import org.apache.calcite.test.schemata.hr.HrSchemaBig;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

/** Tests for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableLimitSort}. */
public class EnumerableLimitSortTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5730">[CALCITE-5730]
   * First nulls can be dropped by EnumerableLimitSort with offset</a>. */
  @Test void nullsFirstWithLimitAndOffset() {
    tester("select commission from emps order by commission nulls first limit 1 offset 1 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "  EnumerableLimitSort(sort0=[$4], dir0=[ASC-nulls-first], offset=[1], fetch=[1])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered("commission=null");
  }

  @Test void nullsLastWithLimitAndOffset() {
    tester("select commission from emps order by commission desc nulls last limit 8 offset 10 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "  EnumerableLimitSort(sort0=[$4], dir0=[DESC-nulls-last], offset=[10], fetch=[8])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=1000",
            "commission=1000",
            "commission=500",
            "commission=500",
            "commission=500",
            "commission=500",
            "commission=500",
            "commission=500");
  }

  @Test void nullsFirstWithLimit() {
    tester("select commission from emps order by commission nulls first limit 13 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "  EnumerableLimitSort(sort0=[$4], dir0=[ASC-nulls-first], fetch=[13])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=null",
            "commission=250");
  }

  @Test void nullsLastWithLimit() {
    tester("select commission from emps order by commission nulls last limit 5 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4])\n"
            + "  EnumerableLimitSort(sort0=[$4], dir0=[ASC], fetch=[5])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=250",
            "commission=250",
            "commission=250",
            "commission=250",
            "commission=250");
  }

  @Test void multiOrderByColumnsWithLimitAndOffset() {
    tester("select commission, salary, empid from emps"
        + " order by commission nulls first, salary asc, empid desc limit 4 offset 6 ")
        .explainContains(
            "EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4], salary=[$t3], empid=[$t0])\n"
            + "  EnumerableLimitSort(sort0=[$4], sort1=[$3], sort2=[$0], dir0=[ASC-nulls-first], dir1=[ASC], dir2=[DESC], offset=[6], fetch=[4])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=null; salary=7000.0; empid=23",
                    "commission=null; salary=7000.0; empid=19",
                    "commission=null; salary=7000.0; empid=15",
                    "commission=null; salary=7000.0; empid=11");
  }

  @Test void multiOrderByColumnsWithLimit() {
    tester("select commission, deptno from emps"
        + " order by commission desc nulls first, deptno asc limit 13 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4], deptno=[$t1])\n"
            + "  EnumerableLimitSort(sort0=[$4], sort1=[$1], dir0=[DESC], dir1=[ASC], fetch=[13])\n"
            + "    EnumerableTableScan(table=[[s, emps]])\n")
        .returnsOrdered(
            "commission=null; deptno=8",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=10",
            "commission=null; deptno=60",
            "commission=null; deptno=80",
            "commission=1000; deptno=10");
  }

  @Test void multiOrderByColumnsNullsLastWithLimitAndOffset() {
    tester("select commission, salary from emps"
        + " order by commission desc nulls last, salary limit 6 offset 12 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4], salary=[$t3])\n"
            + "  EnumerableLimitSort(sort0=[$4], sort1=[$3], dir0=[DESC-nulls-last], dir1=[ASC], offset=[12], fetch=[6])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0",
            "commission=500; salary=8000.0");
  }

  @Test void multiOrderByColumnsNullsLastWithLimit() {
    tester("select commission, empid from emps"
        + " order by commission nulls last, empid desc limit 4 ")
        .explainContains("EnumerableCalc(expr#0..4=[{inputs}], commission=[$t4], empid=[$t0])\n"
            + "  EnumerableLimitSort(sort0=[$4], sort1=[$0], dir0=[ASC], dir1=[DESC], fetch=[4])\n"
            + "    EnumerableTableScan(table=[[s, emps]])")
        .returnsOrdered(
            "commission=250; empid=48",
            "commission=250; empid=44",
            "commission=250; empid=40",
            "commission=250; empid=36");
  }

  private CalciteAssert.AssertQuery tester(String sqlQuery) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .withSchema("s", new ReflectiveSchema(new HrSchemaBig()))
        .query(sqlQuery)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.removeRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
        });
  }
}
