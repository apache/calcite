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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.PigRelBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import org.junit.jupiter.api.Test;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit test for {@link PigRelBuilder}.
 */
class PigRelBuilderTest {
  /** Creates a config based on the "scott" schema. */
  public static Frameworks.ConfigBuilder config() {
    return RelBuilderTest.config();
  }

  static PigRelBuilder createBuilder(
      UnaryOperator<RelBuilder.Config> transform) {
    final Frameworks.ConfigBuilder configBuilder = config();
    configBuilder.context(
        Contexts.of(transform.apply(RelBuilder.Config.DEFAULT)));
    return PigRelBuilder.create(configBuilder.build());
  }

  /** Converts a relational expression to a sting with linux line-endings. */
  private String str(RelNode r) {
    return Util.toLinux(RelOptUtil.toString(r));
  }

  @Test void testScan() {
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

  @Test void testCogroup() {}
  @Test void testCross() {}
  @Test void testCube() {}
  @Test void testDefine() {}
  @Test void testDistinct() {
    // Syntax:
    //   alias = DISTINCT alias [PARTITION BY partitioner] [PARALLEL n];
    final PigRelBuilder builder = PigRelBuilder.create(config().build());
    final RelNode root = builder
        .scan("EMP")
        .project(builder.field("DEPTNO"))
        .distinct()
        .build();
    final String plan = "LogicalAggregate(group=[{0}])\n"
        + "  LogicalProject(DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(plan));
  }

  @Test void testFilter() {
    // Syntax:
    //  FILTER name BY expr
    // Example:
    //  output_var = FILTER input_var BY (field1 is not null);
    final PigRelBuilder builder = PigRelBuilder.create(config().build());
    final RelNode root = builder
        .load("EMP.csv", null, null)
        .filter(builder.isNotNull(builder.field("MGR")))
        .build();
    final String plan = "LogicalFilter(condition=[IS NOT NULL($3)])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(root), is(plan));
  }

  @Test void testForeach() {}

  @Test void testGroup() {
    // Syntax:
    //   alias = GROUP alias { ALL | BY expression}
    //     [, alias ALL | BY expression ...] [USING 'collected' | 'merge']
    //     [PARTITION BY partitioner] [PARALLEL n];
    // Equivalent to Pig Latin:
    //   r = GROUP e BY (deptno, job);
    final Function<PigRelBuilder, RelNode> f = builder ->
        builder.scan("EMP")
            .group(null, null, -1, builder.groupKey("DEPTNO", "JOB").alias("e"))
            .build();
    final String plan = ""
        + "LogicalAggregate(group=[{0, 1}], EMP=[COLLECT($2)])\n"
        + "  LogicalProject(JOB=[$2], DEPTNO=[$7], "
        + "$f8=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(str(f.apply(createBuilder(b -> b))), is(plan));

    // now without pruning
    final String plan2 = ""
        + "LogicalAggregate(group=[{2, 7}], EMP=[COLLECT($8)])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(
        str(f.apply(createBuilder(b -> b.withPruneInputOfAggregate(false)))),
        is(plan2));
  }

  @Test void testGroup2() {
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
    final String plan = "LogicalJoin(condition=[=($0, $2)], joinType=[inner])\n"
        + "  LogicalAggregate(group=[{0}], EMP=[COLLECT($1)])\n"
        + "    LogicalProject(EMPNO=[$0], $f8=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalAggregate(group=[{0}], DEPT=[COLLECT($1)])\n"
        + "    LogicalProject(DEPTNO=[$0], $f3=[ROW($0, $1, $2)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(str(root), is(plan));
  }

  @Test void testImport() {}
  @Test void testJoinInner() {}
  @Test void testJoinOuter() {}
  @Test void testLimit() {}

  @Test void testLoad() {
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

  @Test void testMapReduce() {}
  @Test void testOrderBy() {}
  @Test void testRank() {}
  @Test void testSample() {}
  @Test void testSplit() {}
  @Test void testStore() {}
  @Test void testUnion() {}
}
