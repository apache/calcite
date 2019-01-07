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

import org.apache.calcite.adapter.spark.SparkRel;
import org.apache.calcite.util.Util;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for using Calcite with Spark as an internal engine, as implemented by
 * the {@link org.apache.calcite.adapter.spark} package.
 */
public class SparkAdapterTest {
  private static final String VALUES0 = "(values (1, 'a'), (2, 'b'))";

  private static final String VALUES1 =
      "(values (1, 'a'), (2, 'b')) as t(x, y)";

  private static final String VALUES2 =
      "(values (1, 'a'), (2, 'b'), (1, 'b'), (2, 'c'), (2, 'c')) as t(x, y)";

  private static final String VALUES3 =
      "(values (1, 'a'), (2, 'b')) as v(w, z)";

  private static final String VALUES4 =
      "(values (1, 'a'), (2, 'b'), (3, 'b'), (4, 'c'), (2, 'c')) as t(x, y)";

  private CalciteAssert.AssertQuery sql(String sql) {
    return CalciteAssert.that()
        .with(CalciteAssert.Config.SPARK)
        .query(sql);
  }

  /**
   * Tests a VALUES query evaluated using Spark.
   * There are no data sources.
   */
  @Test public void testValues() {
    // Insert a spurious reference to a class in Calcite's Spark adapter.
    // Otherwise this test doesn't depend on the Spark module at all, and
    // Javadoc gets confused.
    Util.discard(SparkRel.class);

    final String sql = "select *\n"
        + "from " + VALUES0;

    final String plan = "PLAN="
        + "EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])";

    final String expectedResult = "EXPR$0=1; EXPR$1=a\n"
        + "EXPR$0=2; EXPR$1=b\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  /** Tests values followed by filter, evaluated by Spark. */
  @Test public void testValuesFilter() {
    final String sql = "select *\n"
        + "from " + VALUES1 + "\n"
        + "where x < 2";

    final String expectedResult = "X=1; Y=a\n";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[2], expr#3=[<($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  @Test public void testSelectDistinct() {
    final String sql = "select distinct *\n"
        + "from " + VALUES2;

    final String plan = "PLAN="
        + "EnumerableAggregate(group=[{0, 1}])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=1; Y=a\n"
        + "X=1; Y=b\n"
        + "X=2; Y=b\n"
        + "X=2; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  // Tests about grouping and aggregate functions

  @Test public void testGroupBy() {
    final String sql = "select sum(x) as SUM_X, min(y) as MIN_Y, max(y) as MAX_Y, "
        + "count(*) as CNT_Y, count(distinct y) as CNT_DIST_Y\n"
        + "from " + VALUES2 + "\n"
        + "group by x";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER NOT NULL], expr#7=[CAST($t2):CHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], expr#8=[CAST($t3):CHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], expr#9=[CAST($t4):BIGINT NOT NULL], SUM_X=[$t6], MIN_Y=[$t7], MAX_Y=[$t8], CNT_Y=[$t9], CNT_DIST_Y=[$t5])\n"
        + "  EnumerableAggregate(group=[{0}], SUM_X=[MIN($2) FILTER $7], MIN_Y=[MIN($3) FILTER $7], MAX_Y=[MIN($4) FILTER $7], CNT_Y=[MIN($5) FILTER $7], CNT_DIST_Y=[COUNT($1) FILTER $6])\n"
        + "    EnumerableCalc(expr#0..6=[{inputs}], expr#7=[0], expr#8=[=($t6, $t7)], expr#9=[1], expr#10=[=($t6, $t9)], proj#0..5=[{exprs}], $g_0=[$t8], $g_1=[$t10])\n"
        + "      EnumerableAggregate(group=[{0, 1}], groups=[[{0, 1}, {0}]], SUM_X=[$SUM0($0)], MIN_Y=[MIN($1)], MAX_Y=[MAX($1)], CNT_Y=[COUNT()], $g=[GROUPING($0, $1)])\n"
        + "        EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n";

    final String expectedResult = "SUM_X=2; MIN_Y=a; MAX_Y=b; CNT_Y=2; CNT_DIST_Y=2\n"
        + "SUM_X=6; MIN_Y=b; MAX_Y=c; CNT_Y=3; CNT_DIST_Y=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testAggFuncNoGroupBy() {
    final String sql = "select sum(x) as SUM_X, min(y) as MIN_Y, max(y) as MAX_Y, "
        + "count(*) as CNT_Y, count(distinct y) as CNT_DIST_Y\n"
        + "from " + VALUES2;

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..4=[{inputs}], expr#5=[CAST($t3):BIGINT NOT NULL], proj#0..2=[{exprs}], CNT_Y=[$t5], CNT_DIST_Y=[$t4])\n"
        + "  EnumerableAggregate(group=[{}], SUM_X=[MIN($1) FILTER $6], MIN_Y=[MIN($2) FILTER $6], MAX_Y=[MIN($3) FILTER $6], CNT_Y=[MIN($4) FILTER $6], CNT_DIST_Y=[COUNT($0) FILTER $5])\n"
        + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[=($t5, $t6)], expr#8=[1], expr#9=[=($t5, $t8)], proj#0..4=[{exprs}], $g_0=[$t7], $g_1=[$t9])\n"
        + "      EnumerableAggregate(group=[{1}], groups=[[{1}, {}]], SUM_X=[$SUM0($0)], MIN_Y=[MIN($1)], MAX_Y=[MAX($1)], CNT_Y=[COUNT()], $g=[GROUPING($1)])\n"
        + "        EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n";

    final String expectedResult = "SUM_X=8; MIN_Y=a; MAX_Y=c; CNT_Y=5; CNT_DIST_Y=3";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testGroupByOrderByAsc() {
    final String sql = "select x, count(*) as CNT_Y\n"
        + "from " + VALUES2 + "\n"
        + "group by x\n"
        + "order by x asc";

    final String plan = "";

    final String expectedResult = "X=1; CNT_Y=2\n"
        + "X=2; CNT_Y=3\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  @Test public void testGroupByMinMaxCountCountDistinctOrderByAsc() {
    final String sql = "select x, min(y) as MIN_Y, max(y) as MAX_Y, count(*) as CNT_Y, "
        + "count(distinct y) as CNT_DIST_Y\n"
        + "from " + VALUES2 + "\n"
        + "group by x\n"
        + "order by x asc";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..4=[{inputs}], expr#5=[CAST($t1):CHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], expr#6=[CAST($t2):CHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], expr#7=[CAST($t3):BIGINT NOT NULL], X=[$t0], MIN_Y=[$t5], MAX_Y=[$t6], CNT_Y=[$t7], CNT_DIST_Y=[$t4])\n"
        + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
        + "    EnumerableAggregate(group=[{0}], MIN_Y=[MIN($2) FILTER $6], MAX_Y=[MIN($3) FILTER $6], CNT_Y=[MIN($4) FILTER $6], CNT_DIST_Y=[COUNT($1) FILTER $5])\n"
        + "      EnumerableCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[=($t5, $t6)], expr#8=[1], expr#9=[=($t5, $t8)], proj#0..4=[{exprs}], $g_0=[$t7], $g_1=[$t9])\n"
        + "        EnumerableAggregate(group=[{0, 1}], groups=[[{0, 1}, {0}]], MIN_Y=[MIN($1)], MAX_Y=[MAX($1)], CNT_Y=[COUNT()], $g=[GROUPING($0, $1)])\n"
        + "          EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=1; MIN_Y=a; MAX_Y=b; CNT_Y=2; CNT_DIST_Y=2\n"
        + "X=2; MIN_Y=b; MAX_Y=c; CNT_Y=3; CNT_DIST_Y=2\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  @Test public void testGroupByMiMaxCountCountDistinctOrderByDesc() {
    final String sql = "select x, min(y) as MIN_Y, max(y) as MAX_Y, count(*) as CNT_Y, "
        + "count(distinct y) as CNT_DIST_Y\n"
        + "from " + VALUES2 + "\n"
        + "group by x\n"
        + "order by x desc";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..4=[{inputs}], expr#5=[CAST($t1):CHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], expr#6=[CAST($t2):CHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL], expr#7=[CAST($t3):BIGINT NOT NULL], X=[$t0], MIN_Y=[$t5], MAX_Y=[$t6], CNT_Y=[$t7], CNT_DIST_Y=[$t4])\n"
        + "  EnumerableSort(sort0=[$0], dir0=[DESC])\n"
        + "    EnumerableAggregate(group=[{0}], MIN_Y=[MIN($2) FILTER $6], MAX_Y=[MIN($3) FILTER $6], CNT_Y=[MIN($4) FILTER $6], CNT_DIST_Y=[COUNT($1) FILTER $5])\n"
        + "      EnumerableCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[=($t5, $t6)], expr#8=[1], expr#9=[=($t5, $t8)], proj#0..4=[{exprs}], $g_0=[$t7], $g_1=[$t9])\n"
        + "        EnumerableAggregate(group=[{0, 1}], groups=[[{0, 1}, {0}]], MIN_Y=[MIN($1)], MAX_Y=[MAX($1)], CNT_Y=[COUNT()], $g=[GROUPING($0, $1)])\n"
        + "          EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=2; MIN_Y=b; MAX_Y=c; CNT_Y=3; CNT_DIST_Y=2\n"
        + "X=1; MIN_Y=a; MAX_Y=b; CNT_Y=2; CNT_DIST_Y=2\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  @Test public void testGroupByHaving() {
    final String sql = "select x\n"
        + "from " + VALUES2 + "\n"
        + "group by x\n"
        + "having count(*) > 2";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[2], expr#3=[>($t1, $t2)], X=[$t0], $condition=[$t3])\n"
        + "  EnumerableAggregate(group=[{0}], agg#0=[COUNT()])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  // Tests about set operators (UNION, UNION ALL, INTERSECT)

  @Test public void testUnionAll() {
    final String sql = "select *\n"
        + "from " + VALUES1 + "\n"
        + " union all\n"
        + "select *\n"
        + "from " + VALUES2;

    final String plan = "PLAN="
        + "EnumerableUnion(all=[true])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n";

    final String expectedResult = "X=1; Y=a\n"
        + "X=1; Y=a\n"
        + "X=1; Y=b\n"
        + "X=2; Y=b\n"
        + "X=2; Y=b\n"
        + "X=2; Y=c\n"
        + "X=2; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testUnion() {
    final String sql = "select *\n"
        + "from " + VALUES1 + "\n"
        + " union\n"
        + "select *\n"
        + "from " + VALUES2;

    final String plan = "PLAN="
        + "EnumerableUnion(all=[false])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n";

    final String expectedResult = "X=1; Y=a\n"
        + "X=1; Y=b\n"
        + "X=2; Y=b\n"
        + "X=2; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testIntersect() {
    final String sql = "select *\n"
        + "from " + VALUES1 + "\n"
        + " intersect\n"
        + "select *\n"
        + "from " + VALUES2;

    final String plan = "PLAN="
        + "EnumerableIntersect(all=[false])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n";

    final String expectedResult = "X=1; Y=a\n"
        + "X=2; Y=b";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  // Tests about sorting

  @Test public void testSortXAscProjectY() {
    final String sql = "select y\n"
        + "from " + VALUES2 + "\n"
        + "order by x asc";

    final String plan = "PLAN="
        + "EnumerableSort(sort0=[$1], dir0=[ASC])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], Y=[$t1], X=[$t0])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "Y=a\n"
        + "Y=b\n"
        + "Y=b\n"
        + "Y=c\n"
        + "Y=c\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  @Test public void testSortXDescYDescProjectY() {
    final String sql = "select y\n"
        + "from " + VALUES2 + "\n"
        + "order by x desc, y desc";

    final String plan = "PLAN="
        + "EnumerableSort(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[DESC])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], Y=[$t1], X=[$t0])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "Y=c\n"
        + "Y=c\n"
        + "Y=b\n"
        + "Y=b\n"
        + "Y=a\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  @Test public void testSortXDescYAscProjectY() {
    final String sql = "select y\n"
        + "from " + VALUES2 + "\n"
        + "order by x desc, y";

    final String plan = "PLAN="
        + "EnumerableSort(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], Y=[$t1], X=[$t0])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "Y=b\n"
        + "Y=c\n"
        + "Y=c\n"
        + "Y=a\n"
        + "Y=b\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  @Test public void testSortXAscYDescProjectY() {
    final String sql = "select y\n"
        + "from " + VALUES2 + "\n"
        + "order by x, y desc";

    final String plan = "PLAN="
        + "EnumerableSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], Y=[$t1], X=[$t0])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "Y=b\n"
        + "Y=a\n"
        + "Y=c\n"
        + "Y=c\n"
        + "Y=b\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  // Tests involving joins

  @Test public void testJoinProject() {
    final String sql = "select t.y, v.z\n"
        + "from " + VALUES2 + "\n"
        + "  join " + VALUES3 + " on t.x = v.w";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..3=[{inputs}], Y=[$t3], Z=[$t1])\n"
        + "  EnumerableJoin(condition=[=($0, $2)], joinType=[inner])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "Y=a; Z=a\n"
        + "Y=b; Z=a\n"
        + "Y=b; Z=b\n"
        + "Y=c; Z=b\n"
        + "Y=c; Z=b";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testJoinProjectAliasProject() {
    final String sql = "select r.z\n"
        + "from (\n"
        + "  select *\n"
        + "  from " + VALUES2 + "\n"
        + "    join " + VALUES3 + " on t.x = v.w) as r";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..3=[{inputs}], Z=[$t1])\n"
        + "  EnumerableJoin(condition=[=($0, $2)], joinType=[inner])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "Z=a\n"
        + "Z=a\n"
        + "Z=b\n"
        + "Z=b\n"
        + "Z=b";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  // Tests involving LIMIT/OFFSET

  @Test public void testLimit() {
    final String sql = "select *\n"
        + "from " + VALUES2 + "\n"
        + "where x = 1\n"
        + "limit 1";

    final String plan = "PLAN="
        + "EnumerableLimit(fetch=[1])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[=($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }";

    final String expectedResult = "X=1; Y=a";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testOrderByLimit() {
    final String sql = "select *\n"
        + "from " + VALUES2 + "\n"
        + "order by y\n"
        + "limit 1";

    final String plan = "PLAN="
        + "EnumerableLimit(fetch=[1])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=1; Y=a\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  @Test public void testOrderByOffset() {
    final String sql = "select *\n"
        + "from " + VALUES2 + "\n"
        + "order by y\n"
        + "offset 2";

    final String plan = "PLAN="
        + "EnumerableLimit(offset=[2])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=1; Y=b\n"
        + "X=2; Y=c\n"
        + "X=2; Y=c\n";

    sql(sql).returns(expectedResult)
        .explainContains(plan);
  }

  // Tests involving "complex" filters in WHERE clause

  @Test public void testFilterBetween() {
    final String sql = "select *\n"
        + "from " + VALUES4 + "\n"
        + "where x between 3 and 4";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[3], expr#3=[>=($t0, $t2)], expr#4=[4], expr#5=[<=($t0, $t4)], expr#6=[AND($t3, $t5)], proj#0..1=[{exprs}], $condition=[$t6])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 3, 'b' }, { 4, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=3; Y=b\n"
        + "X=4; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testFilterIsIn() {
    final String sql = "select *\n"
        + "from " + VALUES4 + "\n"
        + "where x in (3, 4)";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[3], expr#3=[=($t0, $t2)], expr#4=[4], expr#5=[=($t0, $t4)], expr#6=[OR($t3, $t5)], proj#0..1=[{exprs}], $condition=[$t6])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 3, 'b' }, { 4, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=3; Y=b\n"
        + "X=4; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testFilterTrue() {
    final String sql = "select *\n"
        + "from " + VALUES2 + "\n"
        + "where true";

    final String plan = "PLAN="
        + "EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=1; Y=a\n"
        + "X=1; Y=b\n"
        + "X=2; Y=b\n"
        + "X=2; Y=c\n"
        + "X=2; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testFilterFalse() {
    final String sql = "select *\n"
        + "from " + VALUES2 + "\n"
        + "where false";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[false], proj#0..1=[{exprs}], $condition=[$t2])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testFilterOr() {
    final String sql = "select *\n"
        + "from " + VALUES2 + "\n"
        + "where x = 1 or x = 2";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[=($t0, $t2)], expr#4=[2], expr#5=[=($t0, $t4)], expr#6=[OR($t3, $t5)], proj#0..1=[{exprs}], $condition=[$t6])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=1; Y=a\n"
        + "X=1; Y=b\n"
        + "X=2; Y=b\n"
        + "X=2; Y=c\n"
        + "X=2; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testFilterIsNotNull() {
    final String sql = "select *\n"
        + "from " + VALUES2 + "\n"
        + "where x is not null";

    final String plan = "PLAN="
        + "EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=1; Y=a\n"
        + "X=1; Y=b\n"
        + "X=2; Y=b\n"
        + "X=2; Y=c\n"
        + "X=2; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testFilterIsNull() {
    final String sql = "select *\n"
        + "from " + VALUES2 + "\n"
        + "where x is null";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[false], proj#0..1=[{exprs}], $condition=[$t2])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  // Tests on more complex queries as UNION operands

  @Test public void testUnionWithFilters() {
    final String sql = "select *\n"
        + "from " + VALUES1 + "\n"
        + "where x > 1\n"
        + " union all\n"
        + "select *\n"
        + "from " + VALUES2 + "\n"
        + "where x > 1";

    final String plan = "PLAN="
        + "EnumerableUnion(all=[true])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[>($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[>($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "    EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n";

    final String expectedResult = "X=2; Y=b\n"
        + "X=2; Y=b\n"
        + "X=2; Y=c\n"
        + "X=2; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testUnionWithFiltersProject() {
    final String sql = "select x\n"
        + "from " + VALUES1 + "\n"
        + "where x > 1\n"
        + " union\n"
        + "select x\n"
        + "from " + VALUES2 + "\n"
        + "where x > 1";

    final String plan = "PLAN="
        + "EnumerableAggregate(group=[{0}])\n"
        + "  EnumerableUnion(all=[true])\n"
        + "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[>($t0, $t2)], X=[$t0], $condition=[$t3])\n"
        + "      EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n"
        + "    EnumerableAggregate(group=[{0}])\n"
        + "      EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[>($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "        EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }, { 1, 'b' }, { 2, 'c' }, { 2, 'c' }]])\n\n";

    final String expectedResult = "X=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  // Tests involving arithmetic operators

  @Test public void testArithmeticPlus() {
    final String sql = "select x\n"
        + "from " + VALUES1 + "\n"
        + "where x + 1 > 1";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t0, $t2)], expr#4=[>($t3, $t2)], X=[$t0], $condition=[$t4])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n\n";

    final String expectedResult = "X=1\n"
        + "X=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testArithmeticMinus() {
    final String sql = "select x\n"
        + "from " + VALUES1 + "\n"
        + "where x - 1 > 0";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[-($t0, $t2)], expr#4=[0], expr#5=[>($t3, $t4)], X=[$t0], $condition=[$t5])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n\n";

    final String expectedResult = "X=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testArithmeticMul() {
    final String sql = "select x\n"
        + "from " + VALUES1 + "\n"
        + "where x * x > 1";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[*($t0, $t0)], expr#3=[1], expr#4=[>($t2, $t3)], X=[$t0], $condition=[$t4])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n\n";

    final String expectedResult = "X=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Test public void testArithmeticDiv() {
    final String sql = "select x\n"
        + "from " + VALUES1 + "\n"
        + "where x / x = 1";

    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[/($t0, $t0)], expr#3=[1], expr#4=[=($t2, $t3)], X=[$t0], $condition=[$t4])\n"
        + "  EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n\n";

    final String expectedResult = "X=1\n"
        + "X=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  // Tests involving sub-queries (both correlated and non correlated)

  @Ignore("[CALCITE-2184] java.lang.ClassCastException: RexSubQuery cannot be cast to RexLocalRef")
  @Test public void testFilterExists() {
    final String sql = "select *\n"
        + "from " + VALUES4 + "\n"
        + "where exists (\n"
        + "  select *\n"
        + "  from " + VALUES3 + "\n"
        + "  where w < x\n"
        + ")";

    final String plan = "PLAN=todo\n\n";

    final String expectedResult = "X=2; Y=b\n"
        + "X=2; Y=c\n"
        + "X=3; Y=b\n"
        + "X=4; Y=c";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Ignore("[CALCITE-2184] java.lang.ClassCastException: RexSubQuery cannot be cast to RexLocalRef")
  @Test public void testFilterNotExists() {
    final String sql = "select *\n"
        + "from " + VALUES4 + "\n"
        + "where not exists (\n"
        + "  select *\n"
        + "  from " + VALUES3 + "\n"
        + "  where w > x\n"
        + ")";

    final String plan = "PLAN=todo\n\n";

    final String expectedResult = "X=1; Y=a";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Ignore("[CALCITE-2184] java.lang.ClassCastException: RexSubQuery cannot be cast to RexLocalRef")
  @Test public void testSubQueryAny() {
    final String sql = "select x\n"
        + "from " + VALUES1 + "\n"
        + "where x <= any (\n"
        + "  select x\n"
        + "  from " + VALUES2 + "\n"
        + ")";

    final String plan = "PLAN=todo\n\n";

    final String expectedResult = "X=1\n"
        + "X=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }

  @Ignore("[CALCITE-2184] java.lang.ClassCastException: RexSubQuery cannot be cast to RexLocalRef")
  @Test public void testSubQueryAll() {
    final String sql = "select x\n"
        + "from " + VALUES1 + "\n"
        + "where x <= all (\n"
        + "  select x\n"
        + "  from " + VALUES2 + "\n"
        + ")";

    final String plan = "PLAN=todo\n\n";

    final String expectedResult = "X=2";

    sql(sql).returnsUnordered(expectedResult)
        .explainContains(plan);
  }
}

// End SparkAdapterTest.java
