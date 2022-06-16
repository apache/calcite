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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rules.CoreRules;
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

/** Test for {@link RelFieldTrimmer}. */
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
                builder.count(false, "C", builder.field("SAL")))
            .hints(aggHint)
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(original);

    final String expected = ""
        + "LogicalAggregate(group=[{1}], C=[COUNT($0)])\n"
        + "  LogicalProject(SAL=[$5], DEPTNO=[$7])\n"
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

  @Test void testCalcFieldTrimmer0() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .exchange(RelDistributions.SINGLETON)
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    final HepProgram hepProgram = new HepProgramBuilder().
        addRuleInstance(CoreRules.PROJECT_TO_CALC).build();

    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relNode = hepPlanner.findBestExp();
    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(relNode);

    final String expected = ""
        + "LogicalCalc(expr#0..1=[{inputs}], proj#0..1=[{exprs}])\n"
        + "  LogicalExchange(distribution=[single])\n"
        + "    LogicalCalc(expr#0..1=[{inputs}], proj#0..1=[{exprs}])\n"
        + "      LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testCalcFieldTrimmer1() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"),
                builder.field("DEPTNO"))
            .exchange(RelDistributions.SINGLETON)
            .filter(
                builder.greaterThan(builder.field("EMPNO"),
                    builder.literal(100)))
            .build();

    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .build();

    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relNode = hepPlanner.findBestExp();
    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(relNode);

    final String expected = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[100], expr#4=[>($t0, $t3)], proj#0."
        + ".2=[{exprs}], $condition=[$t4])\n"
        + "  LogicalExchange(distribution=[single])\n"
        + "    LogicalCalc(expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n"
        + "      LogicalProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testCalcFieldTrimmer2() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .exchange(RelDistributions.SINGLETON)
            .filter(
                builder.greaterThan(builder.field("EMPNO"),
                    builder.literal(100)))
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE).build();

    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relNode = hepPlanner.findBestExp();
    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(relNode);

    final String expected = ""
        + "LogicalCalc(expr#0..1=[{inputs}], expr#2=[100], expr#3=[>($t0, $t2)], proj#0."
        + ".1=[{exprs}], $condition=[$t3])\n"
        + "  LogicalExchange(distribution=[single])\n"
        + "    LogicalCalc(expr#0..1=[{inputs}], proj#0..1=[{exprs}])\n"
        + "      LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test void testCalcWithHints() {
    final RelHint calcHint = RelHint.builder("resource").build();
    final RelBuilder builder = RelBuilder.create(config().build());
    builder.getCluster().setHintStrategies(
        HintStrategyTable.builder().hintStrategy("resource", HintPredicates.CALC).build());
    final RelNode original =
        builder.scan("EMP")
            .project(
                builder.field("EMPNO"),
                builder.field("ENAME"),
                builder.field("DEPTNO")
            ).hints(calcHint)
            .sort(builder.field("EMPNO"))
            .project(builder.field("EMPNO"))
            .build();

    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(original);
    final RelNode relNode = hepPlanner.findBestExp();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(relNode);

    final String expected = ""
        + "LogicalCalc(expr#0=[{inputs}], EMPNO=[$t0])\n"
        + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "    LogicalCalc(expr#0=[{inputs}], EMPNO=[$t0])\n"
        + "      LogicalProject(EMPNO=[$0])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));

    assertTrue(original.getInput(0).getInput(0) instanceof Project);
    final Project originalProject = (Project) original.getInput(0).getInput(0);
    assertTrue(originalProject.getHints().contains(calcHint));

    assertTrue(relNode.getInput(0).getInput(0) instanceof Calc);
    final Calc originalCalc = (Calc) relNode.getInput(0).getInput(0);
    assertTrue(originalCalc.getHints().contains(calcHint));

    assertTrue(trimmed.getInput(0).getInput(0) instanceof Calc);
    final Calc calc = (Calc) trimmed.getInput(0).getInput(0);
    assertTrue(calc.getHints().contains(calcHint));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4783">[CALCITE-4783]
   * RelFieldTrimmer incorrectly drops filter condition</a>. */
  @Test void testCalcFieldTrimmer3() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(
                builder.field("ENAME"),
                builder.field("DEPTNO"))
            .exchange(RelDistributions.SINGLETON)
            .filter(builder.equals(builder.field("ENAME"), builder.literal("bob")))
            .aggregate(builder.groupKey(), builder.countStar(null))
            .build();

    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_TO_CALC).build();

    final HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relNode = hepPlanner.findBestExp();
    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(relNode);

    final String expected = ""
        + "LogicalAggregate(group=[{}], agg#0=[COUNT()])\n"
        + "  LogicalCalc(expr#0=[{inputs}], expr#1=['bob'], expr#2=[=($t0, $t1)], $condition=[$t2])\n"
        + "    LogicalExchange(distribution=[single])\n"
        + "      LogicalProject(ENAME=[$1])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4995">[CALCITE-4995]
   * AssertionError caused by RelFieldTrimmer on SEMI/ANTI join</a>. */
  @Test void testSemiJoinAntiJoinFieldTrimmer() {
    for (final JoinRelType joinType : new JoinRelType[]{JoinRelType.ANTI, JoinRelType.SEMI}) {
      final RelBuilder builder = RelBuilder.create(config().build());
      final RelNode root = builder
          .values(new String[]{"id"}, 1, 2).as("a")
          .values(new String[]{"id"}, 2, 3).as("b")
          .join(joinType,
              builder.equals(
                  builder.field(2, "a", "id"),
                  builder.field(2, "b", "id")))
          .values(new String[]{"id"}, 0, 2).as("c")
          .join(joinType,
              builder.equals(
                  builder.field(2, "a", "id"),
                  builder.field(2, "c", "id")))
          .build();

      final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
      final RelNode trimmed = fieldTrimmer.trim(root);
      final String expected = ""
          + "LogicalJoin(condition=[=($0, $1)], joinType=[" + joinType.lowerName + "])\n"
          + "  LogicalJoin(condition=[=($0, $1)], joinType=[" + joinType.lowerName + "])\n"
          + "    LogicalValues(tuples=[[{ 1 }, { 2 }]])\n"
          + "    LogicalValues(tuples=[[{ 2 }, { 3 }]])\n"
          + "  LogicalValues(tuples=[[{ 0 }, { 2 }]])\n";
      assertThat(trimmed, hasTree(expected));
    }
  }

  @Test void testUnionFieldTrimmer() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP").as("t1")
            .project(builder.field("EMPNO"))
            .scan("EMP").as("t2")
            .scan("EMP").as("t3")
            .join(JoinRelType.INNER,
                builder.equals(
                    builder.field(2, "t2", "EMPNO"),
                    builder.field(2, "t3", "EMPNO")))
            .project(builder.field("t2", "EMPNO"))
            .union(false)
            .build();
    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(root);
    final String expected = ""
        + "LogicalUnion(all=[false])\n"
        + "  LogicalProject(EMPNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalProject(EMPNO=[$0])\n"
        + "    LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
        + "      LogicalProject(EMPNO=[$0])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalProject(EMPNO=[$0])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }
}
