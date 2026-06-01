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
import org.apache.calcite.test.schemata.hr.HrSchema;

import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

/** Test for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableSortedAggregate}. */
public class EnumerableSortedAggregateTest {
  @Test void sortedAgg() {
    tester(false, new HrSchema())
        .query("select deptno, "
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
    tester(false, new HrSchema())
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
    tester(false, new HrSchema())
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
    tester(false, new HrSchema())
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
    tester(false, new HrSchema())
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5101">[CALCITE-5101]
   * LISTAGG(DISTINCT ...) WITHIN GROUP fails with ArrayIndexOutOfBoundsException</a>. */
  @Test void listAggDistinctWithinGroupOrderByNonGroupColumn() {
    // FAILING CASE: LISTAGG(DISTINCT ...) WITHIN GROUP (ORDER BY non-group-column)
    // Previously threw: ArrayIndexOutOfBoundsException: Index 2 out of bounds for length 2
    // Now fixed: ORDER BY salary is properly projected and remapped.
    // Within each group, names are ordered by salary (ascending).
    // deptno=10: Sebastian (7000), Bill (10000), Theodore (11500)
    // deptno=20: Eric (8000)
    tester(false, new HrSchema())
        .query("select deptno, "
            + "LISTAGG(DISTINCT name) WITHIN GROUP (ORDER BY salary) as names "
            + "from emps group by deptno")
        .returnsUnordered(
            "deptno=10; names=Sebastian,Bill,Theodore",
            "deptno=20; names=Eric");
  }

  @Test void listAggDistinctWithoutOrderBy() {
    // WORKING CASE: LISTAGG(DISTINCT ...) without ORDER BY - always worked
    tester(false, new HrSchema())
        .query("select deptno, "
            + "LISTAGG(DISTINCT name) as names "
            + "from emps group by deptno")
        .returnsUnordered(
            "deptno=10; names=Bill,Sebastian,Theodore",
            "deptno=20; names=Eric");
  }

  @Test void listAggWithoutDistinctWithinGroupOrderBy() {
    // WORKING CASE: LISTAGG(...) WITHIN GROUP (ORDER BY non-group-column)
    // without DISTINCT - always worked
    tester(false, new HrSchema())
        .query("select deptno, "
            + "LISTAGG(name) WITHIN GROUP (ORDER BY salary) as names "
            + "from emps group by deptno")
        .returnsUnordered(
            "deptno=10; names=Sebastian,Bill,Theodore",
            "deptno=20; names=Eric");
  }

  @Test void listAggDistinctWithinGroupOrderByAggColumn() {
    // WORKING CASE: LISTAGG(DISTINCT ...) WITHIN GROUP (ORDER BY aggregated-column)
    // - always worked because the aggregated column is in the re-grouped input
    tester(false, new HrSchema())
        .query("select deptno, "
            + "LISTAGG(DISTINCT name) WITHIN GROUP (ORDER BY name) as names "
            + "from emps group by deptno")
        .returnsUnordered(
            "deptno=10; names=Bill,Sebastian,Theodore",
            "deptno=20; names=Eric");
  }

  @Test void listAggMultipleDistinctWithinGroupOrderByNonGroupColumn() {
    // Test case for multiple DISTINCT aggregates with different ORDER BY columns
    // Previously threw: ArrayIndexOutOfBoundsException
    // ORDER BY salary for first agg (not a group key), ORDER BY name for second
    // This tests that collation indices are properly remapped in rewriteUsingGroupingSets
    tester(false, new HrSchema())
        .query("select deptno, "
            + "LISTAGG(DISTINCT name) WITHIN GROUP (ORDER BY salary) as names_by_salary, "
            + "LISTAGG(DISTINCT commission) WITHIN GROUP (ORDER BY name) as commissions_by_name "
            + "from emps group by deptno")
        .returnsUnordered(
            "deptno=10; names_by_salary=Theodore,Sebastian,Bill; commissions_by_name=250,1000",
            "deptno=20; names_by_salary=Eric; commissions_by_name=500");
  }

  @Test void groupingSetsWithDistinctAggAndCollationReferencingOutsideGroupingSets() {
    // Test that result collation is safely reduced when ORDER BY references
    // columns not present in all grouping sets.
    // Why result collation can have fewer elements than input collation:
    // - Input collation may reference {salary, deptno, name}
    // - fullGroupSet contains only {deptno, name} (columns in some grouping sets)
    // - salary is NOT in fullGroupSet (not part of any GROUPING SETS combination)
    // - In GROUPING SETS ((deptno), (name), ()), salary has inconsistent values
    //   within each logical group, so sorting by it would be meaningless
    // - Result collation safely drops salary and keeps only {deptno, name}
    // This tests that AggregateExpandDistinctAggregatesRule.remapCollationForGroupingSets
    // correctly filters out columns not in fullGroupSet.
    // The key point: this query should not throw ArrayIndexOutOfBoundsException.
    tester(false, new HrSchema())
        .query("select deptno, name, "
            + "LISTAGG(DISTINCT salary) WITHIN GROUP (ORDER BY salary) as salaries "
            + "from emps "
            + "group by grouping sets ((deptno), (name), ())")
        .returnsUnordered(
            // Grouping by deptno - contains the 3 salaries from deptno=10
            "deptno=10; name=null; salaries=10000.0,7000.0,11500.0",
            "deptno=20; name=null; salaries=8000.0",
            // Grouping by name - single salary per name
            "deptno=null; name=Bill; salaries=10000.0",
            "deptno=null; name=Eric; salaries=8000.0",
            "deptno=null; name=Sebastian; salaries=7000.0",
            "deptno=null; name=Theodore; salaries=11500.0",
            // Grand total - all 4 distinct salaries
            "deptno=null; name=null; salaries=10000.0,8000.0,7000.0,11500.0");
  }

  private CalciteAssert.AssertThat tester(boolean forceDecorrelate,
                                          Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate)
        .withSchema("s", new ReflectiveSchema(schema));
  }
}
