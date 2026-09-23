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
package org.apache.calcite.benchmarks;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.CommonRelSubExprRegisterRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.test.Fixtures;

import com.google.common.collect.ImmutableList;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@code HepPlanner.setRoot} and {@code findBestExp} over a range of query shapes.
 * Queries are converted to {@link RelNode} once per trial, so only planning is measured.
 */
@Fork(value = 1, jvmArgsPrepend = {"-Xss200m",
    "-Dcalcite.disable.generate.type.digest.string=true"})
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Threads(1)
public class HepQueryVarietyBenchmark {

  /** Backstop should a rule set stop reaching a fixpoint; these queries never reach it. */
  private static final int MATCH_LIMIT = 1000;

  @Param({"FILTER_PROJECT", "AGGREGATE", "CORRELATED_EXISTS", "JOIN5", "MIXED_DEEP",
      "UNION_WIDE", "COMMON_SUBEXPR", "UNION_PRUNE_EMPTY"})
  String query;

  /** Collections the rules are split across, one {@code addRuleCollection} call each. */
  @Param({"1", "5", "10"})
  int ruleCollections;

  /** Rules in each collection; one is what {@code addRuleInstance} builds. */
  @Param({"1", "5", "10", "50"})
  int rulesPerCollection;

  /** Share of the rule set able to rewrite these queries, not the share of attempts matching. */
  @Param({"0", "50", "100"})
  int firingPercent;

  /** Rules that rewrite these queries. No rule's inverse is present; a pair never converges. */
  private static final List<RelOptRule> FIRING =
      ImmutableList.of(
          CoreRules.FILTER_INTO_JOIN,
          CoreRules.FILTER_PROJECT_TRANSPOSE,
          CoreRules.FILTER_MERGE,
          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
          CoreRules.PROJECT_MERGE,
          CoreRules.PROJECT_REMOVE,
          CoreRules.AGGREGATE_PROJECT_MERGE,
          CoreRules.AGGREGATE_REMOVE,
          CoreRules.JOIN_CONDITION_PUSH,
          CoreRules.JOIN_PUSH_EXPRESSIONS,
          CoreRules.SORT_PROJECT_TRANSPOSE,
          CoreRules.UNION_MERGE,
          CoreRules.CALC_MERGE);

  /** Real rules no query here can fire: none has INTERSECT, MINUS, MATCH, SAMPLE or exchange. */
  private static final List<RelOptRule> NON_APPLICABLE =
      ImmutableList.of(
          CoreRules.INTERSECT_MERGE,
          CoreRules.INTERSECT_REMOVE,
          CoreRules.INTERSECT_REORDER,
          CoreRules.INTERSECT_TO_DISTINCT,
          CoreRules.INTERSECT_TO_EXISTS,
          CoreRules.INTERSECT_TO_SEMI_JOIN,
          CoreRules.INTERSECT_FILTER_TO_FILTER,
          CoreRules.MINUS_MERGE,
          CoreRules.MINUS_REMOVE,
          CoreRules.MINUS_TO_DISTINCT,
          CoreRules.MINUS_TO_ANTI_JOIN,
          CoreRules.MINUS_FILTER_TO_FILTER,
          CoreRules.SAMPLE_TO_FILTER,
          CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
          CoreRules.MATCH);

  /** Rules that reach the common-subexpression path; none calls {@code transformTo}. */
  private static final List<RelOptRule> COMMON_SUB_EXPR =
      ImmutableList.of(
          CommonRelSubExprRegisterRule.Config.FILTER.toRule(),
          CommonRelSubExprRegisterRule.Config.PROJECT.toRule(),
          CommonRelSubExprRegisterRule.Config.JOIN.toRule(),
          CommonRelSubExprRegisterRule.Config.AGGREGATE.toRule());

  /** One of three core rules with an {@code UNORDERED} child policy. */
  private static final List<RelOptRule> UNORDERED_RULE =
      ImmutableList.of(PruneEmptyRules.UNION_INSTANCE);

  private static String sqlFor(String name) {
    switch (name) {
    case "FILTER_PROJECT":
      return "select empno, ename, sal + comm as total from emp\n"
          + "where deptno = 10 and sal > 1000 and job <> 'CLERK'";
    case "JOIN5":
      return "select e1.ename, e2.ename, d1.name, d2.name, s.grade\n"
          + "from emp e1\n"
          + "join emp e2 on e1.mgr = e2.empno\n"
          + "join dept d1 on e1.deptno = d1.deptno\n"
          + "join dept d2 on e2.deptno = d2.deptno\n"
          + "join salgrade s on e1.sal between s.losal and s.hisal\n"
          + "where d1.deptno <> d2.deptno";
    case "AGGREGATE":
      return "select deptno, job, count(*) as c, sum(sal) as s, avg(sal) as a\n"
          + "from emp where sal > 500\n"
          + "group by deptno, job having sum(sal) > 1000";
    case "CORRELATED_EXISTS":
      return "select e.ename from emp e\n"
          + "where exists (select 1 from dept d where d.deptno = e.deptno and d.name <> 'X')";
    case "MIXED_DEEP":
      return "select t.deptno, t.c, d.name from\n"
          + "  (select deptno, count(*) as c from\n"
          + "     (select * from emp where sal > 100 and job <> 'CLERK') x\n"
          + "   where x.deptno > 5 group by deptno) t\n"
          + "join dept d on t.deptno = d.deptno\n"
          + "where t.c > 1 order by t.c desc";
    case "UNION_WIDE":
      return wideUnion(60);
    case "COMMON_SUBEXPR":
      // Identical branches share a vertex, giving it the two parents this path requires.
      return "select t1.deptno, t1.s, t2.s from\n"
          + "  (select deptno, sum(sal) as s from emp where sal > 100 group by deptno) t1\n"
          + "join\n"
          + "  (select deptno, sum(sal) as s from emp where sal > 100 group by deptno) t2\n"
          + "on t1.deptno = t2.deptno";
    default:
      throw new IllegalArgumentException("unknown query: " + name);
    }
  }

  /**
   * Returns a UNION ALL of {@code branches} filtered scans, giving a plan of a few hundred
   * nodes rather than the few dozen the hand-written queries above produce.
   */
  private static String wideUnion(int branches) {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < branches; i++) {
      if (i > 0) {
        b.append("union all\n");
      }
      b.append("select ename, sal from emp where deptno = ").append(i)
          .append(" and sal > ").append(i).append("\n");
    }
    return b.toString();
  }

  /**
   * Returns a union of 60 real branches plus one empty {@code Values}, built directly with
   * {@link LogicalUnion#create} since SQL has no way to write a single wide union with a
   * provably-empty branch.
   */
  private static RelNode unionPruneEmptyRoot() {
    final List<RelNode> branches = new ArrayList<>(61);
    for (int i = 0; i < 60; i++) {
      branches.add(
          Fixtures.forSqlToRel()
              .withSql("select ename, sal from emp where deptno = " + i + " and sal > " + i)
              .toRel());
    }
    branches.add(
        LogicalValues.createEmpty(branches.get(0).getCluster(), branches.get(0).getRowType()));
    return LogicalUnion.create(branches, true);
  }

  private RelNode root;
  private HepProgram program;

  @Setup(Level.Trial)
  public void setup() {
    root = "UNION_PRUNE_EMPTY".equals(query)
        ? unionPruneEmptyRoot()
        : Fixtures.forSqlToRel().withSql(sqlFor(query)).toRel();

    HepProgramBuilder builder = HepProgram.builder()
        .addMatchLimit(MATCH_LIMIT)
        .addMatchOrder(HepMatchOrder.ARBITRARY);
    for (List<RelOptRule> collection : collections()) {
      builder.addRuleCollection(collection);
    }
    program = builder.build();
  }

  /**
   * Returns {@link #ruleCollections} collections of {@link #rulesPerCollection} rules,
   * {@code firingPercent} of them from {@link #FIRING}, spread evenly so that every
   * collection holds a similar mix rather than the firing rules landing in the first few.
   */
  private List<List<RelOptRule>> collections() {
    final int total = ruleCollections * rulesPerCollection;
    final int firing = total * firingPercent / 100;
    final List<RelOptRule> nonFiring;
    switch (query) {
    case "COMMON_SUBEXPR":
      nonFiring = COMMON_SUB_EXPR;
      break;
    case "UNION_PRUNE_EMPTY":
      nonFiring = UNORDERED_RULE;
      break;
    default:
      nonFiring = NON_APPLICABLE;
    }
    List<RelOptRule> all = new ArrayList<>(total);
    int f = 0;
    int n = 0;
    for (int i = 0; i < total; i++) {
      if ((i + 1) * firing / total > i * firing / total) {
        all.add(FIRING.get(f++ % FIRING.size()));
      } else {
        all.add(nonFiring.get(n++ % nonFiring.size()));
      }
    }
    List<List<RelOptRule>> split = new ArrayList<>(ruleCollections);
    for (int i = 0; i < ruleCollections; i++) {
      split.add(all.subList(i * rulesPerCollection, (i + 1) * rulesPerCollection));
    }
    return split;
  }

  @Benchmark
  public RelNode plan() {
    HepPlanner planner = new HepPlanner(program);
    planner.setLargePlanMode(true);
    planner.setRoot(root);
    return planner.findBestExp();
  }
}
