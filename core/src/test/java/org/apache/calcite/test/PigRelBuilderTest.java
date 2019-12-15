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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.PigRelBuilder;
import org.apache.calcite.util.Util;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test for {@link PigRelBuilder}.
 */
public class PigRelBuilderTest {
  /** Creates a config based on the "scott" schema. */
  public static Frameworks.ConfigBuilder config() {
    return RelBuilderTest.config();
  }

  /** Converts a relational expression to a sting with linux line-endings. */
  private String str(RelNode r) {
    return Util.toLinux(RelOptUtil.toString(r));
  }

  @Test public void testScan() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    final PigRelBuilder builder = PigRelBuilder.create(config().build());
    final RelNode root = builder
        .scan("EMP")
        .build();
    assertThat(str(root),
        is("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testCogroup() {}
  @Test public void testCross() {}
  @Test public void testCube() {}
  @Test public void testDefine() {}
  @Test public void testDistinct() {
    // Syntax:
    //   alias = DISTINCT alias [PARTITION BY partitioner] [PARALLEL n];
    final PigRelBuilder builder = PigRelBuilder.create(config().build());
    final RelNode root = builder
        .scan("EMP")
        .project(builder.field("DEPTNO"))
        .distinct()
        .build();
    final String plan = "LogicalAggregate(group=[{0}])\n"
        + "  LogicalProject(DEPTNO=[_7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(plan));
  }

  @Test public void testFilter() {
    // Syntax:
    //  FILTER name BY expr
    // Example:
    //  output_var = FILTER input_var BY (field1 is not null);
    final PigRelBuilder builder = PigRelBuilder.create(config().build());
    final RelNode root = builder
        .load("EMP.csv", null, null)
        .filter(builder.isNotNull(builder.field("MGR")))
        .build();
    final String plan = "LogicalFilter(condition=[IS NOT NULL(_3)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(plan));
  }

  @Test public void testForeach() {}

  @Test public void testGroup() {
    // Syntax:
    //   alias = GROUP alias { ALL | BY expression}
    //     [, alias ALL | BY expression ...] [USING 'collected' | 'merge']
    //     [PARTITION BY partitioner] [PARALLEL n];
    // Equivalent to Pig Latin:
    //   r = GROUP e BY (deptno, job);
    final PigRelBuilder builder = PigRelBuilder.create(config().build());
    final RelNode root = builder
        .scan("EMP")
        .group(null, null, -1, builder.groupKey("DEPTNO", "JOB").alias("e"))
        .build();
    final String plan = ""
        + "LogicalAggregate(group=[{2, 7}], EMP=[COLLECT(_8)])\n"
        + "  LogicalProject(EMPNO=[_0], ENAME=[_1], JOB=[_2], MGR=[_3], HIREDATE=[_4], SAL=[_5], COMM=[_6], DEPTNO=[_7], _f8=[ROW(_0, _1, _2, _3, _4, _5, _6, _7)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(plan));
  }

  @Test public void testGroup2() {
    // Equivalent to Pig Latin:
    //   r = GROUP e BY deptno, d BY deptno;
    final PigRelBuilder builder = PigRelBuilder.create(config().build());
    final RelNode root = builder
        .scan("EMP")
        .scan("DEPT")
        .group(null, null, -1,
            builder.groupKey("DEPTNO").alias("e"),
            builder.groupKey("DEPTNO").alias("d"))
        .build();
    final String plan = "LogicalJoin(condition=[=(_0, _2)], joinType=[inner])\n"
        + "  LogicalAggregate(group=[{0}], EMP=[COLLECT(_8)])\n"
        + "    LogicalProject(EMPNO=[_0], ENAME=[_1], JOB=[_2], MGR=[_3], HIREDATE=[_4], SAL=[_5], COMM=[_6], DEPTNO=[_7], _f8=[ROW(_0, _1, _2, _3, _4, _5, _6, _7)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n  LogicalAggregate(group=[{0}], DEPT=[COLLECT(_3)])\n"
        + "    LogicalProject(DEPTNO=[_0], DNAME=[_1], LOC=[_2], _f3=[ROW(_0, _1, _2)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(plan));
  }

  @Test public void testImport() {}
  @Test public void testJoinInner() {}
  @Test public void testJoinOuter() {}
  @Test public void testLimit() {}

  @Test public void testLoad() {
    // Syntax:
    //   LOAD 'data' [USING function] [AS schema];
    // Equivalent to Pig Latin:
    //   LOAD 'EMPS.csv'
    final PigRelBuilder builder = PigRelBuilder.create(config().build());
    final RelNode root = builder
        .load("EMP.csv", null, null)
        .build();
    assertThat(str(root),
        is("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  @Test public void testMapReduce() {}
  @Test public void testOrderBy() {}
  @Test public void testRank() {}
  @Test public void testSample() {}
  @Test public void testSplit() {}
  @Test public void testStore() {}
  @Test public void testUnion() {}
}
