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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit test for
 * {@link org.apache.calcite.rel.rules.materialize.MaterializedViewRule} and its
 * sub-classes, in which materialized views are matched to the structure of a
 * plan.
 */
class MaterializedViewRelOptRulesTest {
  static final MaterializedViewTester TESTER =
      new MaterializedViewTester() {
        @Override protected List<RelNode> optimize(RelNode queryRel,
            List<RelOptMaterialization> materializationList) {
          RelOptPlanner planner = queryRel.getCluster().getPlanner();
          RelTraitSet traitSet = queryRel.getCluster().traitSet()
              .replace(EnumerableConvention.INSTANCE);
          RelOptUtil.registerDefaultRules(planner, true, false);
          return ImmutableList.of(
              Programs.standard().run(planner, queryRel, traitSet,
                  materializationList, ImmutableList.of()));
        }
      };

  /** Creates a fixture. */
  protected MaterializedViewFixture fixture(String query) {
    return MaterializedViewFixture.create(query, TESTER);
  }

  /** Creates a fixture with a given query. */
  protected final MaterializedViewFixture sql(String materialize,
      String query) {
    return fixture(query)
        .withMaterializations(ImmutableList.of(Pair.of(materialize, "MV0")));
  }

  @Test void testSwapJoin() {
    sql("select count(*) as c from \"foodmart\".\"sales_fact_1997\" as s"
            + " join \"foodmart\".\"time_by_day\" as t on s.\"time_id\" = t.\"time_id\"",
        "select count(*) as c from \"foodmart\".\"time_by_day\" as t"
            + " join \"foodmart\".\"sales_fact_1997\" as s on t.\"time_id\" = s.\"time_id\"")
        .withDefaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .ok();
  }

  /** Aggregation materialization with a project. */
  @Test void testAggregateProject() {
    // Note that materialization does not start with the GROUP BY columns.
    // Not a smart way to design a materialization, but people may do it.
    sql("select \"deptno\", count(*) as c, \"empid\" + 2, sum(\"empid\") as s "
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select count(*) + 1 as c, \"deptno\" from \"emps\" group by \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], C=[$t3], deptno=[$t0])\n"
            + "  EnumerableAggregate(group=[{0}], agg#0=[$SUM0($1)])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs1() {
    sql("select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"",
        "select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"").ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs2() {
    sql("select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs3() {
    sql("select \"deptno\" from \"emps\" group by \"deptno\"",
        "select \"empid\", \"deptno\" from \"emps\" group by \"empid\", \"deptno\"")
        .noMat();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs4() {
    sql("select \"empid\", \"deptno\"\n"
            + "from \"emps\" where \"deptno\" = 10 group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" where \"deptno\" = 10 group by \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs5() {
    sql("select \"empid\", \"deptno\"\n"
            + "from \"emps\" where \"deptno\" = 5 group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" where \"deptno\" = 10 group by \"deptno\"")
        .noMat();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs6() {
    sql("select \"empid\", \"deptno\"\n"
            + "from \"emps\" where \"deptno\" > 5 group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" where \"deptno\" > 10 group by \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[10], expr#3=[<($t2, $t1)], proj#0..1=[{exprs}], $condition=[$t3])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testAggregateMaterializationNoAggregateFuncs7() {
    sql("select \"empid\", \"deptno\"\n"
            + "from \"emps\" where \"deptno\" > 5 group by \"empid\", \"deptno\"",
        "select \"deptno\" from \"emps\" where \"deptno\" < 10 group by \"deptno\"")
        .noMat();
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
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs2() {
    sql("select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}], C=[$SUM0($2)], S=[$SUM0($3)])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs3() {
    sql("select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select \"deptno\", \"empid\", sum(\"empid\") as s, count(*) as c\n"
            + "from \"emps\" group by \"empid\", \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], deptno=[$t1], empid=[$t0], S=[$t3], C=[$t2])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs4() {
    sql("select \"empid\", \"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}], S=[$SUM0($3)])\n"
            + "  EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs5() {
    sql("select \"empid\", \"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" where \"deptno\" >= 10 group by \"empid\", \"deptno\"",
        "select \"deptno\", sum(\"empid\") + 1 as s\n"
            + "from \"emps\" where \"deptno\" > 10 group by \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)],"
            + " deptno=[$t0], S=[$t3])\n"
            + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])")
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
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t0, $t2)], "
            + "expr#4=[+($t1, $t2)], EXPR$0=[$t3], S=[$t4])\n"
            + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])")
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
    sql("select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month), "
            + "count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\"\n"
            + "group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs10() {
    sql("select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month), "
            + "count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\"\n"
            + "group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(\"empid\") + 1 as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs11() {
    sql("select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to second), "
            + "count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\"\n"
            + "group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to second)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to minute), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to minute)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs12() {
    sql("select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to second), "
            + "count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\"\n"
            + "group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to second)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to month), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to month)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs13() {
    sql("select \"empid\", cast('1997-01-20 12:34:56' as timestamp), "
            + "count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\"\n"
            + "group by \"empid\", cast('1997-01-20 12:34:56' as timestamp)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to year), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to year)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs14() {
    sql("select \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month), "
            + "count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\"\n"
            + "group by \"empid\", floor(cast('1997-01-20 12:34:56' as timestamp) to month)",
        "select floor(cast('1997-01-20 12:34:56' as timestamp) to hour), sum(\"empid\") as s\n"
            + "from \"emps\" group by floor(cast('1997-01-20 12:34:56' as timestamp) to hour)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs15() {
    sql("select \"eventid\", floor(cast(\"ts\" as timestamp) to second), "
            + "count(*) + 1 as c, sum(\"eventid\") as s\n"
            + "from \"events\" group by \"eventid\", floor(cast(\"ts\" as timestamp) to second)",
        "select floor(cast(\"ts\" as timestamp) to minute), sum(\"eventid\") as s\n"
            + "from \"events\" group by floor(cast(\"ts\" as timestamp) to minute)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs16() {
    sql("select \"eventid\", cast(\"ts\" as timestamp), count(*) + 1 as c, sum(\"eventid\") as s\n"
            + "from \"events\" group by \"eventid\", cast(\"ts\" as timestamp)",
        "select floor(cast(\"ts\" as timestamp) to year), sum(\"eventid\") as s\n"
            + "from \"events\" group by floor(cast(\"ts\" as timestamp) to year)")
        .ok();
  }

  @Test void testAggregateMaterializationAggregateFuncs17() {
    sql("select \"eventid\", floor(cast(\"ts\" as timestamp) to month), "
            + "count(*) + 1 as c, sum(\"eventid\") as s\n"
            + "from \"events\" group by \"eventid\", floor(cast(\"ts\" as timestamp) to month)",
        "select floor(cast(\"ts\" as timestamp) to hour), sum(\"eventid\") as s\n"
            + "from \"events\" group by floor(cast(\"ts\" as timestamp) to hour)")
        .checkingThatResultContains("EnumerableTableScan(table=[[hr, events]])")
        .ok();
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

  @Test void testAggregateMaterializationAggregateFuncs20() {
    sql("select 11 as \"empno\", 22 as \"sal\", count(*) from \"emps\" group by 11, 22",
        "select * from\n"
            + "(select 11 as \"empno\", 22 as \"sal\", count(*)\n"
            + "from \"emps\" group by 11, 22) tmp\n"
            + "where \"sal\" = 33")
        .checkingThatResultContains("EnumerableValues(tuples=[[]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs1() {
    sql("select \"empid\", \"depts\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[<($t2, $t1)], "
            + "empid=[$t0], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs2() {
    sql("select \"depts\".\"deptno\", \"empid\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"depts\".\"deptno\" > 10\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"depts\".\"deptno\" > 20\n"
            + "group by \"empid\", \"depts\".\"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[<($t2, $t0)], "
            + "empid=[$t1], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
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
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[20], expr#3=[<($t2, $t1)], "
            + "empid=[$t0], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs5() {
    sql("select \"depts\".\"deptno\", \"emps\".\"empid\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 10\n"
            + "group by \"depts\".\"deptno\", \"emps\".\"empid\"",
        "select \"depts\".\"deptno\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 15\n"
            + "group by \"depts\".\"deptno\", \"emps\".\"empid\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[15], expr#3=[<($t2, $t1)], "
            + "deptno=[$t0], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs6() {
    sql("select \"depts\".\"deptno\", \"emps\".\"empid\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 10\n"
            + "group by \"depts\".\"deptno\", \"emps\".\"empid\"",
        "select \"depts\".\"deptno\" from \"depts\"\n"
            + "join \"emps\" using (\"deptno\") where \"emps\".\"empid\" > 15\n"
            + "group by \"depts\".\"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{0}])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[15], expr#3=[<($t2, $t1)], "
            + "proj#0..1=[{exprs}], $condition=[$t3])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs7() {
    sql("select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 11\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10\n"
            + "group by \"dependents\".\"empid\"")
        .checkingThatResultContains("EnumerableAggregate(group=[{0}])",
                "EnumerableUnion(all=[true])",
                "EnumerableAggregate(group=[{2}])",
                "EnumerableTableScan(table=[[hr, MV0]])",
                "expr#5=[Sarg[(10..11]]], expr#6=[SEARCH($t0, $t5)]")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs8() {
    sql("select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 20\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
            + "group by \"dependents\".\"empid\"")
        .noMat();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs9() {
    sql("select \"depts\".\"deptno\", \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
            + "group by \"dependents\".\"empid\"")
        .checkingThatResultContains("EnumerableAggregate(group=[{0}])",
            "EnumerableUnion(all=[true])",
            "EnumerableAggregate(group=[{2}])",
            "EnumerableTableScan(table=[[hr, MV0]])",
            "expr#5=[Sarg[(10..11], [19..20)]], expr#6=[SEARCH($t0, $t5)]")
        .ok();
  }

  @Test void testJoinAggregateMaterializationNoAggregateFuncs10() {
    sql("select \"depts\".\"name\", \"dependents\".\"name\" as \"name2\", "
            + "\"emps\".\"deptno\", \"depts\".\"deptno\" as \"deptno2\", "
            + "\"dependents\".\"empid\"\n"
            + "from \"depts\", \"dependents\", \"emps\"\n"
            + "where \"depts\".\"deptno\" > 10\n"
            + "group by \"depts\".\"name\", \"dependents\".\"name\", "
            + "\"emps\".\"deptno\", \"depts\".\"deptno\", "
            + "\"dependents\".\"empid\"",
        "select \"dependents\".\"empid\"\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10\n"
            + "group by \"dependents\".\"empid\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{4}])\n"
            + "  EnumerableCalc(expr#0..4=[{inputs}], expr#5=[=($t2, $t3)], "
            + "expr#6=[CAST($t1):VARCHAR], "
            + "expr#7=[CAST($t0):VARCHAR], "
            + "expr#8=[=($t6, $t7)], expr#9=[AND($t5, $t8)], proj#0..4=[{exprs}], $condition=[$t9])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs1() {
    // This test relies on FK-UK relationship
    sql("select \"empid\", \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"deptno\" from \"emps\" group by \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs2() {
    sql("select \"empid\", \"emps\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"emps\".\"deptno\"",
        "select \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"depts\".\"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}], C=[$SUM0($2)], S=[$SUM0($3)])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs3() {
    // This test relies on FK-UK relationship
    sql("select \"empid\", \"depts\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "group by \"empid\", \"depts\".\"deptno\"",
        "select \"deptno\", \"empid\", sum(\"empid\") as s, count(*) as c\n"
            + "from \"emps\" group by \"empid\", \"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], deptno=[$t1], empid=[$t0], S=[$t3], C=[$t2])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs4() {
    sql("select \"empid\", \"emps\".\"deptno\", count(*) as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"emps\".\"deptno\" >= 10 group by \"empid\", \"emps\".\"deptno\"",
        "select \"depts\".\"deptno\", sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"emps\".\"deptno\" > 10 group by \"depts\".\"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{1}], S=[$SUM0($3)])\n"
            + "  EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs5() {
    sql("select \"empid\", \"depts\".\"deptno\", count(*) + 1 as c, sum(\"empid\") as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"depts\".\"deptno\" >= 10 group by \"empid\", \"depts\".\"deptno\"",
        "select \"depts\".\"deptno\", sum(\"empid\") + 1 as s\n"
            + "from \"emps\" join \"depts\" using (\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 group by \"depts\".\"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], "
            + "deptno=[$t0], S=[$t3])\n"
            + "  EnumerableAggregate(group=[{1}], agg#0=[$SUM0($3)])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t4, $t1)], "
            + "proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
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
    sql("select \"dependents\".\"empid\", \"emps\".\"deptno\", sum(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"dependents\".\"empid\", sum(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{0}], S=[$SUM0($2)])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])\n"
            + "    EnumerableTableScan(table=[[hr, depts]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs8() {
    sql("select \"dependents\".\"empid\", \"emps\".\"deptno\", sum(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"depts\".\"name\", sum(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"depts\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"depts\".\"name\"")
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{4}], S=[$SUM0($2)])\n"
            + "  EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]])\n"
            + "    EnumerableTableScan(table=[[hr, depts]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs9() {
    sql("select \"dependents\".\"empid\", \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..2=[{inputs}], deptno=[$t1], S=[$t2])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs10() {
    sql("select \"dependents\".\"empid\", \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"emps\".\"deptno\"")
        .noMat();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs11() {
    sql("select \"depts\".\"deptno\", \"dependents\".\"empid\", count(\"emps\".\"salary\") as s\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\", count(\"emps\".\"salary\") + 1\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
            + "group by \"dependents\".\"empid\"")
        .checkingThatResultContains("EnumerableCalc(expr#0..1=[{inputs}], "
                + "expr#2=[1], expr#3=[+($t1, $t2)], empid=[$t0], EXPR$1=[$t3])\n"
                + "  EnumerableAggregate(group=[{0}], agg#0=[$SUM0($1)])",
            "EnumerableUnion(all=[true])",
            "EnumerableAggregate(group=[{2}], agg#0=[COUNT()])",
            "EnumerableAggregate(group=[{1}], agg#0=[$SUM0($2)])",
            "EnumerableTableScan(table=[[hr, MV0]])",
            "expr#5=[Sarg[(10..11], [19..20)]], expr#6=[SEARCH($t0, $t5)]")
        .ok();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs12() {
    sql("select \"depts\".\"deptno\", \"dependents\".\"empid\", "
            + "count(distinct \"emps\".\"salary\") as s\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 11 and \"depts\".\"deptno\" < 19\n"
            + "group by \"depts\".\"deptno\", \"dependents\".\"empid\"",
        "select \"dependents\".\"empid\", count(distinct \"emps\".\"salary\") + 1\n"
            + "from \"depts\"\n"
            + "join \"dependents\" on (\"depts\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"locations\" on (\"locations\".\"name\" = \"dependents\".\"name\")\n"
            + "join \"emps\" on (\"emps\".\"deptno\" = \"depts\".\"deptno\")\n"
            + "where \"depts\".\"deptno\" > 10 and \"depts\".\"deptno\" < 20\n"
            + "group by \"dependents\".\"empid\"")
        .noMat();
  }

  @Test void testJoinAggregateMaterializationAggregateFuncs13() {
    sql("select \"dependents\".\"empid\", \"emps\".\"deptno\", count(distinct \"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"",
        "select \"emps\".\"deptno\", count(\"salary\") as s\n"
            + "from \"emps\"\n"
            + "join \"dependents\" on (\"emps\".\"empid\" = \"dependents\".\"empid\")\n"
            + "group by \"dependents\".\"empid\", \"emps\".\"deptno\"")
        .noMat();
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4276">[CALCITE-4276]
   * If query contains join and rollup function (FLOOR), rewrite to materialized
   * view contains bad field offset</a>. */
  @Test void testJoinAggregateMaterializationAggregateFuncs15() {
    final String m = ""
        + "SELECT \"deptno\",\n"
        + "  COUNT(*) AS \"dept_size\",\n"
        + "  SUM(\"salary\") AS \"dept_budget\"\n"
        + "FROM \"emps\"\n"
        + "GROUP BY \"deptno\"";
    final String q = ""
        + "SELECT FLOOR(\"CREATED_AT\" TO YEAR) AS by_year,\n"
        + "  COUNT(*) AS \"num_emps\"\n"
        + "FROM (SELECT\"deptno\"\n"
        + "    FROM \"emps\") AS \"t\"\n"
        + "JOIN (SELECT \"deptno\",\n"
        + "        \"inceptionDate\" as \"CREATED_AT\"\n"
        + "    FROM \"depts2\") using (\"deptno\")\n"
        + "GROUP BY FLOOR(\"CREATED_AT\" TO YEAR)";
    String plan = ""
        + "EnumerableAggregate(group=[{8}], num_emps=[$SUM0($1)])\n"
        + "  EnumerableCalc(expr#0..7=[{inputs}], expr#8=[FLAG(YEAR)], "
        + "expr#9=[FLOOR($t3, $t8)], proj#0..7=[{exprs}], $f8=[$t9])\n"
        + "    EnumerableHashJoin(condition=[=($0, $4)], joinType=[inner])\n"
        + "      EnumerableTableScan(table=[[hr, MV0]])\n"
        + "      EnumerableTableScan(table=[[hr, depts2]])\n";
    sql(m, q)
        .checkingThatResultContains(plan)
        .ok();
  }

  @Test void testJoinMaterialization1() {
    String q = "select *\n"
        + "from (select * from \"emps\" where \"empid\" < 300)\n"
        + "join \"depts\" using (\"deptno\")";
    sql("select * from \"emps\" where \"empid\" < 500", q).ok();
  }

  @Disabled
  @Test void testJoinMaterialization2() {
    String q = "select *\n"
        + "from \"emps\"\n"
        + "join \"depts\" using (\"deptno\")";
    String m = "select \"deptno\", \"empid\", \"name\",\n"
        + "\"salary\", \"commission\" from \"emps\"";
    sql(m, q).ok();
  }

  @Test void testJoinMaterialization3() {
    String q = "select \"empid\" \"deptno\" from \"emps\"\n"
        + "join \"depts\" using (\"deptno\") where \"empid\" = 1";
    String m = "select \"empid\" \"deptno\" from \"emps\"\n"
        + "join \"depts\" using (\"deptno\")";
    sql(m, q).ok();
  }

  @Test void testJoinMaterialization4() {
    sql("select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" = 1")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):INTEGER NOT NULL], expr#2=[1], "
            + "expr#3=[=($t1, $t2)], deptno=[$t0], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinMaterialization5() {
    sql("select cast(\"empid\" as BIGINT) from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" > 1")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):JavaType(int) NOT NULL], "
            + "expr#2=[1], expr#3=[<($t2, $t1)], EXPR$0=[$t1], $condition=[$t3])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinMaterialization6() {
    sql("select cast(\"empid\" as BIGINT) from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" \"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\") where \"empid\" = 1")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):JavaType(int) NOT NULL], "
            + "expr#2=[1], expr#3=[CAST($t1):INTEGER NOT NULL], expr#4=[=($t2, $t3)], "
            + "EXPR$0=[$t1], $condition=[$t4])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
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
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..2=[{inputs}], empid=[$t1])\n"
            + "  EnumerableHashJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR], name=[$t1])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])\n"
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
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t2])\n"
            + "  EnumerableHashJoin(condition=[=($1, $4)], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[CAST($t0):VARCHAR], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])\n"
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
        .checkingThatResultContains("EnumerableUnion(all=[true])",
                "EnumerableTableScan(table=[[hr, MV0]])",
                "expr#5=[Sarg[(10..30]]], expr#6=[SEARCH($t0, $t5)]")
        .ok();
  }

  @Test void testJoinMaterialization11() {
    sql("select \"empid\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" from \"emps\"\n"
            + "where \"deptno\" in (select \"deptno\" from \"depts\")")
        .noMat();
  }

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
        "select \"a\".\"empid\" from \n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")")
        .ok();
  }

  @Test void testJoinMaterializationUKFK2() {
    sql("select \"a\".\"empid\", \"a\".\"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"empid\" from \n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinMaterializationUKFK3() {
    sql("select \"a\".\"empid\", \"a\".\"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"a\".\"name\" from \n"
            + "(select * from \"emps\" where \"empid\" = 1) \"a\"\n"
            + "join \"dependents\" using (\"empid\")\n")
        .noMat();
  }

  @Test void testJoinMaterializationUKFK4() {
    sql("select \"empid\" \"deptno\" from\n"
            + "(select * from \"emps\" where \"empid\" = 1)\n"
            + "join \"depts\" using (\"deptno\")",
        "select \"empid\" from \"emps\" where \"empid\" = 1\n")
        .ok();
  }

  @Test void testJoinMaterializationUKFK5() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" using (\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
        .ok();
  }

  @Test void testJoinMaterializationUKFK6() {
    sql("select \"emps\".\"empid\", \"emps\".\"deptno\" from \"emps\"\n"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "join \"depts\" \"b\" on (\"emps\".\"deptno\"=\"b\".\"deptno\")\n"
            + "join \"dependents\" using (\"empid\")"
            + "where \"emps\".\"empid\" = 1",
        "select \"emps\".\"empid\" from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")\n"
            + "where \"emps\".\"empid\" = 1")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], empid=[$t0])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]])")
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

  @Test void testJoinMaterializationUKFK9() {
    sql("select * from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")",
        "select \"emps\".\"empid\", \"dependents\".\"empid\", \"emps\".\"deptno\"\n"
            + "from \"emps\"\n"
            + "join \"dependents\" using (\"empid\")"
            + "join \"depts\" \"a\" on (\"emps\".\"deptno\"=\"a\".\"deptno\")\n"
            + "where \"emps\".\"name\" = 'Bill'")
        .ok();
  }

  @Test void testQueryProjectWithBetween() {
    sql("select *"
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " where s.\"store_id\" = 1",
        "select s.\"time_id\" between 1 and 3"
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " where s.\"store_id\" = 1")
        .withDefaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..7=[{inputs}], expr#8=[Sarg[[1..3]]], "
            + "expr#9=[SEARCH($t1, $t8)], $f0=[$t9])\n"
            + "  EnumerableTableScan(table=[[foodmart, MV0]])")
        .ok();
  }

  @Test void testJoinQueryProjectWithBetween() {
    sql("select *"
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " join \"foodmart\".\"time_by_day\" as t on s.\"time_id\" = t.\"time_id\""
            + " where s.\"store_id\" = 1",
        "select s.\"time_id\" between 1 and 3"
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " join \"foodmart\".\"time_by_day\" as t on s.\"time_id\" = t.\"time_id\""
            + " where s.\"store_id\" = 1")
        .withDefaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..17=[{inputs}], expr#18=[Sarg[[1..3]]], "
            + "expr#19=[SEARCH($t8, $t18)], $f0=[$t19])\n"
            + "  EnumerableTableScan(table=[[foodmart, MV0]])")
        .ok();
  }

  @Test void testViewProjectWithBetween() {
    sql("select s.\"time_id\", s.\"time_id\" between 1 and 3"
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " where s.\"store_id\" = 1",
        "select s.\"time_id\""
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " where s.\"store_id\" = 1")
        .withDefaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], time_id=[$t0])\n"
            + "  EnumerableTableScan(table=[[foodmart, MV0]])")
        .ok();
  }

  @Test void testQueryAndViewProjectWithBetween() {
    sql("select s.\"time_id\", s.\"time_id\" between 1 and 3"
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " where s.\"store_id\" = 1",
        "select s.\"time_id\" between 1 and 3"
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " where s.\"store_id\" = 1")
        .withDefaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], EXPR$1=[$t1])\n"
            + "  EnumerableTableScan(table=[[foodmart, MV0]])")
        .ok();
  }

  @Test void testViewProjectWithMultifieldExpressions() {
    sql("select s.\"time_id\", s.\"time_id\" >= 1 and s.\"time_id\" < 3,"
            + " s.\"time_id\" >= 1 or s.\"time_id\" < 3, "
            + " s.\"time_id\" + s.\"time_id\", "
            + " s.\"time_id\" * s.\"time_id\""
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " where s.\"store_id\" = 1",
        "select s.\"time_id\""
            + " from \"foodmart\".\"sales_fact_1997\" as s"
            + " where s.\"store_id\" = 1")
        .withDefaultSchemaSpec(CalciteAssert.SchemaSpec.JDBC_FOODMART)
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..4=[{inputs}], time_id=[$t0])\n"
            + "  EnumerableTableScan(table=[[foodmart, MV0]])")
        .ok();
  }

  @Test void testAggregateOnJoinKeys() {
    sql("select \"deptno\", \"empid\", \"salary\" "
            + "from \"emps\"\n"
            + "group by \"deptno\", \"empid\", \"salary\"",
        "select \"empid\", \"depts\".\"deptno\" "
            + "from \"emps\"\n"
            + "join \"depts\" on \"depts\".\"deptno\" = \"empid\" group by \"empid\", \"depts\".\"deptno\"")
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0=[{inputs}], empid=[$t0], empid0=[$t0])\n"
            + "  EnumerableAggregate(group=[{1}])\n"
            + "    EnumerableHashJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])\n"
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
        .checkingThatResultContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n"
            + "  EnumerableAggregate(group=[{1}], EXPR$0=[$SUM0($3)])\n"
            + "    EnumerableHashJoin(condition=[=($1, $4)], joinType=[inner])\n"
            + "      EnumerableTableScan(table=[[hr, MV0]])\n"
            + "      EnumerableTableScan(table=[[hr, depts]])")
        .ok();
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
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{0}], C=[COUNT($1)])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]]")
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
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{0}], C=[COUNT($2)])\n"
            + "  EnumerableTableScan(table=[[hr, MV0]]")
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
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT($1)])\n"
            + "  EnumerableAggregate(group=[{0, 2}])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]]")
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
        .checkingThatResultContains(""
            + "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT()])\n"
            + "  EnumerableAggregate(group=[{0, 1}])\n"
            + "    EnumerableTableScan(table=[[hr, MV0]]")
        .ok();
  }
}
