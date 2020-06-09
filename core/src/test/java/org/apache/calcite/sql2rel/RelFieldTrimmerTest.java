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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelFieldTrimmerTest {
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test void testSortExchangeFieldTrimmer() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .sortExchange(RelDistributions.hash(Lists.newArrayList(1)), RelCollations.of(0))
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalSortExchange(distribution=[hash[1]], collation=[[0]])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testSortExchangeFieldTrimmerWhenProjectCannotBeMerged() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .sortExchange(RelDistributions.hash(Lists.newArrayList(1)), RelCollations.of(0))
            .project(builder.field("EMPNO"))
            .build();

    RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalProject(EMPNO=[$0])\n"
        + "  LogicalSortExchange(distribution=[hash[1]], collation=[[0]])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testSortExchangeFieldTrimmerWithEmptyCollation() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .sortExchange(RelDistributions.hash(Lists.newArrayList(1)), RelCollations.EMPTY)
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalSortExchange(distribution=[hash[1]], collation=[[]])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testSortExchangeFieldTrimmerWithSingletonDistribution() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .sortExchange(RelDistributions.SINGLETON, RelCollations.of(0))
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalSortExchange(distribution=[single], collation=[[0]])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testExchangeFieldTrimmer() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .exchange(RelDistributions.hash(Lists.newArrayList(1)))
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalExchange(distribution=[hash[1]])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testExchangeFieldTrimmerWhenProjectCannotBeMerged() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .exchange(RelDistributions.hash(Lists.newArrayList(1)))
            .project(builder.field("EMPNO"))
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalProject(EMPNO=[$0])\n"
        + "  LogicalExchange(distribution=[hash[1]])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testExchangeFieldTrimmerWithSingletonDistribution() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .exchange(RelDistributions.SINGLETON)
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalExchange(distribution=[single])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4055">[CALCITE-4055]
   * RelFieldTrimmer loses hints</a>. */
  @Test void testJoinWithHints() {
    final RelHint noHashJoinHint = RelHint.builder("no_hash_join").build();
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.getCluster().setHintStrategies(
        HintStrategyTable.builder()
            .hintStrategy("no_hash_join", HintPredicates.JOIN)
            .build());
    final RelNode original =
        builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.INNER,
                builder.equals(
                    builder.field(2, 0, "DEPTNO"),
                    builder.field(2, 1, "DEPTNO")))
            .hints(noHashJoinHint)
            .project(
                builder.field("ENAME"),
                builder.field("DNAME"))
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(original);

    final String expected = ""
        + "LogicalProject(ENAME=[$1], DNAME=[$4])\n"
        + "  LogicalJoin(condition=[=($2, $3)], joinType=[inner])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(trimmed, hasTree(expected));

    assertTrue(original.getInput(0) instanceof Join);
    final Join originalJoin = (Join) original.getInput(0);
    assertTrue(originalJoin.getHints().contains(noHashJoinHint));

    assertTrue(trimmed.getInput(0) instanceof Join);
    final Join join = (Join) trimmed.getInput(0);
    assertTrue(join.getHints().contains(noHashJoinHint));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4055">[CALCITE-4055]
   * RelFieldTrimmer loses hints</a>. */
  @Test void testAggregateWithHints() {
    final RelHint aggHint = RelHint.builder("resource").build();
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.getCluster().setHintStrategies(
        HintStrategyTable.builder().hintStrategy("resource", HintPredicates.AGGREGATE).build());
    final RelNode original =
        builder.scan("EMP")
            .aggregate(
                builder.groupKey(builder.field("DEPTNO")),
                builder.count(false, "C", builder.field("EMPNO")))
            .hints(aggHint)
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(original);

    final String expected = ""
        + "LogicalAggregate(group=[{1}], C=[COUNT($0)])\n"
        + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));

    assertTrue(original instanceof Aggregate);
    final Aggregate originalAggregate = (Aggregate) original;
    assertTrue(originalAggregate.getHints().contains(aggHint));

    assertTrue(trimmed instanceof Aggregate);
    final Aggregate aggregate = (Aggregate) trimmed;
    assertTrue(aggregate.getHints().contains(aggHint));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4055">[CALCITE-4055]
   * RelFieldTrimmer loses hints</a>. */
  @Test void testProjectWithHints() {
    final RelHint projectHint = RelHint.builder("resource").build();
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.getCluster().setHintStrategies(
        HintStrategyTable.builder().hintStrategy("resource", HintPredicates.PROJECT).build());
    final RelNode original =
        builder.scan("EMP")
            .project(
                builder.field("EMPNO"),
                builder.field("ENAME"),
                builder.field("DEPTNO")
            ).hints(projectHint)
            .sort(builder.field("EMPNO"))
            .project(builder.field("EMPNO"))
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(original);

    final String expected = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(EMPNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));

    assertTrue(original.getInput(0).getInput(0) instanceof Project);
    final Project originalProject = (Project) original.getInput(0).getInput(0);
    assertTrue(originalProject.getHints().contains(projectHint));

    assertTrue(trimmed.getInput(0) instanceof Project);
    final Project project = (Project) trimmed.getInput(0);
    assertTrue(project.getHints().contains(projectHint));
  }

}
