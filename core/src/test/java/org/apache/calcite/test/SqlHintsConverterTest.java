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
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.test.Matchers.relIsValid;
import static org.apache.calcite.test.SqlToRelTestBase.NL;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import static java.util.Objects.requireNonNull;

/**
 * Unit test for {@link org.apache.calcite.rel.hint.RelHint}.
 */
class SqlHintsConverterTest {

  static final Fixture FIXTURE =
      new Fixture(SqlTestFactory.INSTANCE,
          DiffRepository.lookup(SqlHintsConverterTest.class),
          "?", false, false)
          .withFactory(f ->
              f.withSqlToRelConfig(c ->
                  c.withHintStrategyTable(HintTools.HINT_STRATEGY_TABLE)));

  static final RelOptFixture RULE_FIXTURE =
      RelOptFixture.DEFAULT
          .withDiffRepos(DiffRepository.lookup(SqlHintsConverterTest.class))
          .withConfig(c ->
              c.withHintStrategyTable(HintTools.HINT_STRATEGY_TABLE));

  protected Fixture fixture() {
    return FIXTURE;
  }

  protected RelOptFixture ruleFixture() {
    return RULE_FIXTURE;
  }

  /** Sets the SQL statement for a test. */
  public final Fixture sql(String sql) {
    return fixture().sql(sql);
  }

  //~ Tests ------------------------------------------------------------------

  @Test void testQueryHint() {
    final String sql = HintTools.withHint("select /*+ %s */ *\n"
        + "from emp e1\n"
        + "inner join dept d1 on e1.deptno = d1.deptno\n"
        + "inner join emp e2 on e1.ename = e2.job");
    sql(sql).ok();
  }

  @Test void testQueryHintWithLiteralOptions() {
    final String sql = "select /*+ time_zone(1, 1.23, 'a bc', -1.0) */ *\n"
        + "from emp";
    sql(sql).ok();
  }

  @Test void testNestedQueryHint() {
    final String sql = "select /*+ resource(parallelism='3'), repartition(10) */ empno\n"
        + "from (select /*+ resource(mem='20Mb')*/ empno, ename from emp)";
    sql(sql).ok();
  }

  @Test void testTwoLevelNestedQueryHint() {
    final String sql = "select /*+ resource(parallelism='3'), no_hash_join */ empno\n"
        + "from (select /*+ resource(mem='20Mb')*/ empno, ename\n"
        + "from emp left join dept on emp.deptno = dept.deptno)";
    sql(sql).ok();
  }

  @Test void testThreeLevelNestedQueryHint() {
    final String sql = "select /*+ index(idx1), no_hash_join */ * from emp /*+ index(empno) */\n"
        + "e1 join dept/*+ index(deptno) */ d1 on e1.deptno = d1.deptno\n"
        + "join emp e2 on d1.name = e2.job";
    sql(sql).ok();
  }

  @Test void testFourLevelNestedQueryHint() {
    final String sql = "select /*+ index(idx1), no_hash_join */ * from emp /*+ index(empno) */\n"
        + "e1 join dept/*+ index(deptno) */ d1 on e1.deptno = d1.deptno join\n"
        + "(select max(sal) as sal from emp /*+ index(empno) */) e2 on e1.sal = e2.sal";
    sql(sql).ok();
  }

  @Test void testAggregateHints() {
    final String sql = "select /*+ AGG_STRATEGY(TWO_PHASE), RESOURCE(mem='1024') */\n"
        + "count(deptno), avg_sal from (\n"
        + "select /*+ AGG_STRATEGY(ONE_PHASE) */ avg(sal) as avg_sal, deptno\n"
        + "from emp group by deptno) group by avg_sal";
    sql(sql).ok();
  }

  @Test void testCorrelateHints() {
    final String sql = "select /*+ use_hash_join (orders, products_temporal) */ stream *\n"
        + "from orders join products_temporal for system_time as of orders.rowtime\n"
        + "on orders.productid = products_temporal.productid and orders.orderId is not null";
    sql(sql).ok();
  }

  @Test void testCrossCorrelateHints() {
    final String sql = "select /*+ use_hash_join (orders, products_temporal) */ stream *\n"
        + "from orders, products_temporal for system_time as of orders.rowtime";
    sql(sql).ok();
  }

  @Test void testFilterHints() {
    final String sql = "select /*+ resource(parallelism='3') */ avg(sal) as avg_sal, deptno\n"
            + "from emp group by deptno having avg(sal) > 5000";
    sql(sql).ok();
  }

  @Test void testUnionHints() {
    final String sql = "select /*+ breakable */ deptno from\n"
            + "(select ename, deptno from emp\n"
            + "union all\n"
            + "select name, deptno from dept)";
    sql(sql).ok();
  }

  @Test void testMinusHints() {
    final String sql = "select /*+ breakable */ deptno from\n"
        + "(select ename, deptno from emp\n"
        + "except all\n"
        + "select name, deptno from dept)";
    sql(sql).ok();
  }

  @Test void testIntersectHints() {
    final String sql = "select /*+ breakable */ deptno from\n"
        + "(select ename, deptno from emp\n"
        + "intersect all\n"
        + "select name, deptno from dept)";
    sql(sql).ok();
  }

  @Test void testSortHints() {
    final String sql = "select /*+ async_merge */ empno from emp order by empno, empno desc";
    sql(sql).ok();
  }

  @Test void testValuesHints() {
    final String sql = "select /*+ resource(parallelism='3') */ a, max(b), max(b + 1)\n"
        + "from (values (1, 2)) as t(a, b)\n"
        + "group by a";
    sql(sql).ok();
  }

  @Test void testWindowHints() {
    final String sql = "select /*+ mini_batch */ last_value(deptno)\n"
        + "over (order by empno rows 2 following) from emp";
    sql(sql).ok();
  }

  @Test void testSnapshotHints() {
    final String sql = "select /*+ fast_snapshot(products_temporal) */ stream * from\n"
        + " orders join products_temporal for system_time as of timestamp '2022-08-11 15:00:00'\n"
        + " on orders.productid = products_temporal.productid";
    sql(sql).ok();
  }

  @Test void testHintsInSubQueryWithDecorrelation() {
    final String sql = "select /*+ resource(parallelism='3'), AGG_STRATEGY(TWO_PHASE) */\n"
        + "sum(e1.empno) from emp e1, dept d1\n"
        + "where e1.deptno = d1.deptno\n"
        + "and e1.sal> (\n"
        + "select /*+ resource(cpu='2') */ avg(e2.sal) from emp e2 where e2.deptno = d1.deptno)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testHintsInSubQueryWithDecorrelation2() {
    final String sql = "select /*+ properties(k1='v1', k2='v2'), index(ename), no_hash_join */\n"
        + "sum(e1.empno) from emp e1, dept d1\n"
        + "where e1.deptno = d1.deptno\n"
        + "and e1.sal> (\n"
        + "select /*+ properties(k1='v1', k2='v2'), index(ename), no_hash_join */\n"
        + "  avg(e2.sal)\n"
        + "  from emp e2\n"
        + "  where e2.deptno = d1.deptno)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testHintsInSubQueryWithDecorrelation3() {
    final String sql = "select /*+ resource(parallelism='3'), index(ename), no_hash_join */\n"
        + "sum(e1.empno) from emp e1, dept d1\n"
        + "where e1.deptno = d1.deptno\n"
        + "and e1.sal> (\n"
        + "select /*+ resource(cpu='2'), index(ename), no_hash_join */\n"
        + "  avg(e2.sal)\n"
        + "  from emp e2\n"
        + "  where e2.deptno = d1.deptno)";
    sql(sql).withDecorrelate(true).ok();
  }

  @Test void testHintsInSubQueryWithoutDecorrelation() {
    final String sql = "select /*+ resource(parallelism='3') */\n"
        + "sum(e1.empno) from emp e1, dept d1\n"
        + "where e1.deptno = d1.deptno\n"
        + "and e1.sal> (\n"
        + "select /*+ resource(cpu='2') */ avg(e2.sal) from emp e2 where e2.deptno = d1.deptno)";
    sql(sql).ok();
  }

  @Test void testInvalidQueryHint() {
    final String sql = "select /*+ weird_hint */ empno\n"
        + "from (select /*+ resource(mem='20Mb')*/ empno, ename\n"
        + "from emp left join dept on emp.deptno = dept.deptno)";
    sql(sql).warns("Hint: WEIRD_HINT should be registered in the HintStrategyTable");

    final String sql1 = "select /*+ resource(mem='20Mb')*/ empno\n"
        + "from (select /*+ weird_kv_hint(k1='v1') */ empno, ename\n"
        + "from emp left join dept on emp.deptno = dept.deptno)";
    sql(sql1).warns("Hint: WEIRD_KV_HINT should be registered in the HintStrategyTable");

    final String sql2 = "select /*+ AGG_STRATEGY(OPTION1) */\n"
        + "ename, avg(sal)\n"
        + "from emp group by ename";
    final String error2 = "Hint AGG_STRATEGY only allows single option, "
        + "allowed options: [ONE_PHASE, TWO_PHASE]";
    sql(sql2).warns(error2);
    // Change the error handler to validate again.
    sql(sql2).withFactory(f ->
        f.withSqlToRelConfig(c ->
            c.withHintStrategyTable(
                HintTools.createHintStrategies(
                    HintStrategyTable.builder().errorHandler(Litmus.THROW)))))
        .fails(error2);
  }

  @Test void testTableHintsInJoin() {
    final String sql = "select\n"
        + "ename, job, sal, dept.name\n"
        + "from emp /*+ index(idx1, idx2) */\n"
        + "join dept /*+ properties(k1='v1', k2='v2') */\n"
        + "on emp.deptno = dept.deptno";
    sql(sql).ok();
  }

  @Test void testTableHintsInSelect() {
    final String sql = HintTools.withHint("select * from emp /*+ %s */");
    sql(sql).ok();
  }

  @Test void testSameHintsWithDifferentInheritPath() {
    final String sql = "select /*+ properties(k1='v1', k2='v2') */\n"
        + "ename, job, sal, dept.name\n"
        + "from emp /*+ index(idx1, idx2) */\n"
        + "join dept /*+ properties(k1='v1', k2='v2') */\n"
        + "on emp.deptno = dept.deptno";
    sql(sql).ok();
  }

  @Test void testTableHintsInInsert() throws Exception {
    final String sql = HintTools.withHint("insert into dept /*+ %s */ (deptno, name) "
        + "select deptno, name from dept");
    final SqlInsert insert = (SqlInsert) sql(sql).parseQuery();
    assert insert.getTargetTable() instanceof SqlTableRef;
    final SqlTableRef tableRef = (SqlTableRef) insert.getTargetTable();
    List<RelHint> hints = SqlUtil.getRelHint(HintTools.HINT_STRATEGY_TABLE,
        (SqlNodeList) tableRef.getOperandList().get(1));
    assertHintsEquals(
        Arrays.asList(
          HintTools.PROPS_HINT,
          HintTools.IDX_HINT,
          HintTools.JOIN_HINT),
        hints);
  }

  @Test void testTableHintsInUpdate() throws Exception {
    final String sql = HintTools.withHint("update emp /*+ %s */ "
        + "set name = 'test' where deptno = 1");
    final SqlUpdate sqlUpdate = (SqlUpdate) sql(sql).parseQuery();
    assert sqlUpdate.getTargetTable() instanceof SqlTableRef;
    final SqlTableRef tableRef = (SqlTableRef) sqlUpdate.getTargetTable();
    List<RelHint> hints = SqlUtil.getRelHint(HintTools.HINT_STRATEGY_TABLE,
        (SqlNodeList) tableRef.getOperandList().get(1));
    assertHintsEquals(
        Arrays.asList(
          HintTools.PROPS_HINT,
          HintTools.IDX_HINT,
          HintTools.JOIN_HINT),
        hints);
  }

  @Test void testTableHintsInDelete() throws Exception {
    final String sql = HintTools.withHint("delete from emp /*+ %s */ where deptno = 1");
    final SqlDelete sqlDelete = (SqlDelete) sql(sql).parseQuery();
    assert sqlDelete.getTargetTable() instanceof SqlTableRef;
    final SqlTableRef tableRef = (SqlTableRef) sqlDelete.getTargetTable();
    List<RelHint> hints = SqlUtil.getRelHint(HintTools.HINT_STRATEGY_TABLE,
        (SqlNodeList) tableRef.getOperandList().get(1));
    assertHintsEquals(
        Arrays.asList(
          HintTools.PROPS_HINT,
          HintTools.IDX_HINT,
          HintTools.JOIN_HINT),
        hints);
  }

  @Test void testTableHintsInMerge() throws Exception {
    final String sql = "merge into emps\n"
        + "/*+ %s */ e\n"
        + "using tempemps as t\n"
        + "on e.empno = t.empno\n"
        + "when matched then update\n"
        + "set name = t.name, deptno = t.deptno, salary = t.salary * .1\n"
        + "when not matched then insert (name, dept, salary)\n"
        + "values(t.name, 10, t.salary * .15)";
    final String sql1 = HintTools.withHint(sql);

    final SqlMerge sqlMerge = (SqlMerge) sql(sql1).parseQuery();
    assert sqlMerge.getTargetTable() instanceof SqlTableRef;
    final SqlTableRef tableRef = (SqlTableRef) sqlMerge.getTargetTable();
    List<RelHint> hints = SqlUtil.getRelHint(HintTools.HINT_STRATEGY_TABLE,
        (SqlNodeList) tableRef.getOperandList().get(1));
    assertHintsEquals(
        Arrays.asList(
            HintTools.PROPS_HINT,
            HintTools.IDX_HINT,
            HintTools.JOIN_HINT),
        hints);
  }

  @Test void testInvalidTableHints() {
    final String sql = "select\n"
        + "ename, job, sal, dept.name\n"
        + "from emp /*+ weird_hint(idx1, idx2) */\n"
        + "join dept /*+ properties(k1='v1', k2='v2') */\n"
        + "on emp.deptno = dept.deptno";
    sql(sql).warns("Hint: WEIRD_HINT should be registered in the HintStrategyTable");

    final String sql1 = "select\n"
        + "ename, job, sal, dept.name\n"
        + "from emp /*+ index(idx1, idx2) */\n"
        + "join dept /*+ weird_kv_hint(k1='v1', k2='v2') */\n"
        + "on emp.deptno = dept.deptno";
    sql(sql1).warns("Hint: WEIRD_KV_HINT should be registered in the HintStrategyTable");
  }

  @Test void testJoinHintRequiresSpecificInputs() {
    final String sql = "select /*+ use_hash_join(r, s), use_hash_join(emp, dept) */\n"
        + "ename, job, sal, dept.name\n"
        + "from emp join dept on emp.deptno = dept.deptno";
    // Hint use_hash_join(r, s) expect to be ignored by the join node.
    sql(sql).ok();
  }

  @Test void testHintsForCalc() {
    final String sql = "select /*+ resource(mem='1024MB')*/ ename, sal, deptno from emp";
    final RelNode rel = sql(sql).toRel();
    final RelHint hint = RelHint.builder("RESOURCE")
        .hintOption("MEM", "1024MB")
        .build();
    // planner rule to convert Project to Calc.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(rel);
    RelNode newRel = planner.findBestExp();
    new ValidateHintVisitor(hint, Calc.class).go(newRel);
  }

  @Test void testHintsPropagationInHepPlannerRules() {
    final String sql = "select /*+ use_hash_join(r, s), use_hash_join(emp, dept) */\n"
        + "ename, job, sal, dept.name\n"
        + "from emp join dept on emp.deptno = dept.deptno";
    final RelNode rel = sql(sql).toRel();
    final RelHint hint = RelHint.builder("USE_HASH_JOIN")
        .inheritPath(0)
        .hintOption("EMP")
        .hintOption("DEPT")
        .build();
    // Validate Hep planner.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(MockJoinRule.INSTANCE)
        .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(rel);
    RelNode newRel = planner.findBestExp();
    new ValidateHintVisitor(hint, Join.class).go(newRel);
  }

  @Test void testHintsPropagationInVolcanoPlannerRules() {
    final String sql = "select /*+ use_hash_join(r, s), use_hash_join(emp, dept) */\n"
        + "ename, job, sal, dept.name\n"
        + "from emp join dept on emp.deptno = dept.deptno";
    final RelHint hint = RelHint.builder("USE_HASH_JOIN")
        .inheritPath(0)
        .hintOption("EMP")
        .hintOption("DEPT")
        .build();
    // Validate Volcano planner.
    RuleSet ruleSet = RuleSets.ofList(
        MockEnumerableJoinRule.create(hint), // Rule to validate the hint.
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_MERGE,
        CoreRules.PROJECT_MERGE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_FILTER_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        EnumerableRules.ENUMERABLE_LIMIT_RULE,
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    ruleFixture()
        .sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(p, false, false);
          ruleSet.forEach(p::addRule);
        })
        .check();
  }

  @Test void testHintsPropagationInVolcanoPlannerRules2() {
    final String sql = "select /*+ no_hash_join */ ename, job\n"
        + "from emp where not exists (select 1 from dept where emp.deptno = dept.deptno)";
    final RelHint hint = RelHint.builder("NO_HASH_JOIN")
        .inheritPath(0, 0)
        .build();
    // Validate Volcano planner.
    RuleSet ruleSet = RuleSets.ofList(
        MockEnumerableJoinRule.create(hint) // Rule to validate the hint.
    );
    ruleFixture()
        .sql(sql)
        .withTrim(true)
        .withVolcanoPlanner(false, p -> {
          p.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(p, false, false);
          ruleSet.forEach(p::addRule);
        })
        .check();
  }

  @Test void testHintsPropagationInVolcanoPlannerRules3() {
    final String sql = "select /*+ no_hash_join */ ename, job\n"
        + "from emp where not exists (select 1 from dept where emp.deptno = dept.deptno) order by ename";
    final RelHint hint = RelHint.builder("NO_HASH_JOIN")
        .inheritPath(0, 0, 0)
        .build();
    // Validate Volcano planner.
    RuleSet ruleSet = RuleSets.ofList(
        MockEnumerableJoinRule.create(hint) // Rule to validate the hint.
    );
    ruleFixture()
        .sql(sql)
        .withTrim(true)
        .withVolcanoPlanner(false, p -> {
          p.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(p, false, false);
          ruleSet.forEach(p::addRule);
        })
        .check();
  }

  @Test void testHintsPropagateWithDifferentKindOfRels() {
    final String sql = "select /*+ AGG_STRATEGY(TWO_PHASE) */\n"
        + "ename, avg(sal)\n"
        + "from emp group by ename";
    final RelNode rel = sql(sql).toRel();
    final RelHint hint = RelHint.builder("AGG_STRATEGY")
        .inheritPath(0)
        .hintOption("TWO_PHASE")
        .build();
    // AggregateReduceFunctionsRule does the transformation:
    // AGG -> PROJECT + AGG
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(rel);
    RelNode newRel = planner.findBestExp();
    new ValidateHintVisitor(hint, Aggregate.class).go(newRel);
  }

  @Test void testUseMergeJoin() {
    final String sql = "select /*+ use_merge_join(emp, dept) */\n"
        + "ename, job, sal, dept.name\n"
        + "from emp join dept on emp.deptno = dept.deptno";
    RuleSet ruleSet = RuleSets.ofList(
        EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
        EnumerableRules.ENUMERABLE_SORT_RULE,
        AbstractConverter.ExpandConversionRule.INSTANCE);

    ruleFixture()
        .sql(sql)
        .withVolcanoPlanner(false, planner -> {
          planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          ruleSet.forEach(planner::addRule);
        })
        .check();
  }

  @Test void testHintExcludeRules() {
    final String sql = "select empno from (select * from "
            + "(select /*+ preserved_project */ empno, ename, deptno from emp)"
            + " where deptno = 20)";

    final RelNode rel = ruleFixture().sql(sql).toRel();
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(rel);
    RelNode newRel = planner.findBestExp();
    Assertions.assertTrue(rel.deepEquals(newRel),
        "Expected:\n"
        + RelOptUtil.toString(rel) + "Computed:\n"
        + RelOptUtil.toString(newRel));
  }

  //~ Methods ----------------------------------------------------------------

  private static boolean equalsStringList(List<String> l, List<String> r) {
    if (l.size() != r.size()) {
      return false;
    }
    for (String s : l) {
      if (!r.contains(s)) {
        return false;
      }
    }
    return true;
  }

  private static void assertHintsEquals(List<RelHint> expected, List<RelHint> actual) {
    assertArrayEquals(expected.toArray(new RelHint[0]), actual.toArray(new RelHint[0]));
  }

  //~ Inner Class ------------------------------------------------------------

  /** A Mock rule to validate the hint. */
  public static class MockJoinRule extends RelRule<MockJoinRule.Config> {
    public static final MockJoinRule INSTANCE = ImmutableMockJoinRuleConfig.builder()
        .build()
        .withOperandSupplier(b ->
            b.operand(LogicalJoin.class).anyInputs())
        .withDescription("MockJoinRule")
        .as(Config.class)
        .toRule();

    MockJoinRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalJoin join = call.rel(0);
      assertThat(join.getHints().size(), is(1));
      call.transformTo(
          LogicalJoin.create(join.getLeft(),
              join.getRight(),
              join.getHints(),
              join.getCondition(),
              join.getVariablesSet(),
              join.getJoinType()));
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(typeImmutable = "ImmutableMockJoinRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default MockJoinRule toRule() {
        return new MockJoinRule(this);
      }
    }
  }

  /** A Mock rule to validate the hint.
   * This rule also converts the rel to EnumerableConvention. */
  private static class MockEnumerableJoinRule extends ConverterRule {
    static MockEnumerableJoinRule create(RelHint hint) {
      return Config.INSTANCE
          .withConversion(LogicalJoin.class, Convention.NONE,
              EnumerableConvention.INSTANCE, "MockEnumerableJoinRule")
          .withRuleFactory(c -> new MockEnumerableJoinRule(c, hint))
          .toRule(MockEnumerableJoinRule.class);
    }

    MockEnumerableJoinRule(Config config, RelHint hint) {
      super(config);
      this.expectedHint = hint;
    }

    private final RelHint expectedHint;

    @Override public RelNode convert(RelNode rel) {
      LogicalJoin join = (LogicalJoin) rel;
      assertThat(join.getHints().size(), is(1));
      assertThat(join.getHints().get(0), is(expectedHint));
      List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (!(input.getConvention() instanceof EnumerableConvention)) {
          input =
            convert(
              input,
              input.getTraitSet()
                .replace(EnumerableConvention.INSTANCE));
        }
        newInputs.add(input);
      }
      final RelOptCluster cluster = join.getCluster();
      final RelNode left = newInputs.get(0);
      final RelNode right = newInputs.get(1);
      final JoinInfo info = join.analyzeCondition();
      return EnumerableHashJoin.create(
        left,
        right,
        info.getEquiCondition(left, right, cluster.getRexBuilder()),
        join.getVariablesSet(),
        join.getJoinType());
    }
  }

  /** A visitor to validate a hintable node has specific hint. **/
  private static class ValidateHintVisitor extends RelVisitor {
    private final RelHint expectedHint;
    private final Class<?> clazz;

    /**
     * Creates the validate visitor.
     *
     * @param hint  the hint to validate
     * @param clazz the node type to validate the hint with
     */
    ValidateHintVisitor(RelHint hint, Class<?> clazz) {
      this.expectedHint = hint;
      this.clazz = clazz;
    }

    @Override public void visit(
        RelNode node,
        int ordinal,
        @Nullable RelNode parent) {
      if (clazz.isInstance(node)) {
        Hintable rel = (Hintable) node;
        assertThat(rel.getHints().size(), is(1));
        assertThat(rel.getHints().get(0), is(expectedHint));
      }
      super.visit(node, ordinal, parent);
    }
  }

  /** Test fixture. */
  private static class Fixture {
    private final String sql;
    private final DiffRepository diffRepos;
    private final SqlTestFactory factory;
    private final SqlTester tester = SqlToRelFixture.TESTER;
    private final List<String> hintsCollect = new ArrayList<>();
    private final boolean decorrelate;
    private final boolean trim;

    Fixture(SqlTestFactory factory, DiffRepository diffRepos, String sql,
        boolean decorrelate, boolean trim) {
      this.factory = requireNonNull(factory, "factory");
      this.sql = requireNonNull(sql, "sql");
      this.diffRepos = requireNonNull(diffRepos, "diffRepos");
      this.decorrelate = decorrelate;
      this.trim = trim;
    }

    Fixture sql(String sql) {
      return new Fixture(factory, diffRepos, sql, decorrelate, trim);
    }

    /** Creates a new Sql instance with new factory
     * applied with the {@code transform}. */
    Fixture withFactory(UnaryOperator<SqlTestFactory> transform) {
      final SqlTestFactory factory = transform.apply(this.factory);
      return new Fixture(factory, diffRepos, sql, decorrelate, trim);
    }

    Fixture withDecorrelate(boolean decorrelate) {
      return new Fixture(factory, diffRepos, sql, decorrelate, trim);
    }

    void ok() {
      assertHintsEquals(sql, "${hints}");
    }

    private void assertHintsEquals(
        String sql,
        String hint) {
      diffRepos.assertEquals("sql", "${sql}", sql);
      String sql2 = diffRepos.expand("sql", sql);
      final RelNode rel =
          tester.convertSqlToRel(factory, sql2, decorrelate, trim)
              .project();

      assertNotNull(rel);
      assertThat(rel, relIsValid());

      final HintCollector collector = new HintCollector(hintsCollect);
      rel.accept(collector);
      StringBuilder builder = new StringBuilder(NL);
      for (String hintLine : hintsCollect) {
        builder.append(hintLine).append(NL);
      }
      diffRepos.assertEquals("hints", hint, builder.toString());
    }

    void fails(String failedMsg) {
      try {
        tester.convertSqlToRel(factory, sql, decorrelate, trim);
        fail("Unexpected exception");
      } catch (AssertionError e) {
        assertThat(e.getMessage(), is(failedMsg));
      }
    }

    void warns(String expectWarning) {
      MockAppender appender = new MockAppender();
      MockLogger logger = new MockLogger();
      logger.addAppender(appender);
      try {
        tester.convertSqlToRel(factory, sql, decorrelate, trim);
      } finally {
        logger.removeAppender(appender);
      }
      appender.loggingEvents.add(expectWarning); // TODO: remove
      assertThat(expectWarning, is(in(appender.loggingEvents)));
    }

    SqlNode parseQuery() throws Exception {
      return tester.parseQuery(factory, sql);
    }

    RelNode toRel() {
      return tester.convertSqlToRel(factory, sql, decorrelate, trim).rel;
    }

    /** A shuttle to collect all the hints within the relational expression into a collection. */
    private static class HintCollector extends RelShuttleImpl {
      private final List<String> hintsCollect;

      HintCollector(List<String> hintsCollect) {
        this.hintsCollect = hintsCollect;
      }

      @Override public RelNode visit(TableScan scan) {
        if (scan.getHints().size() > 0) {
          this.hintsCollect.add("TableScan:" + scan.getHints());
        }
        return super.visit(scan);
      }

      @Override public RelNode visit(LogicalJoin join) {
        if (join.getHints().size() > 0) {
          this.hintsCollect.add("LogicalJoin:" + join.getHints());
        }
        return super.visit(join);
      }

      @Override public RelNode visit(LogicalProject project) {
        if (project.getHints().size() > 0) {
          this.hintsCollect.add("Project:" + project.getHints());
        }
        return super.visit(project);
      }

      @Override public RelNode visit(LogicalAggregate aggregate) {
        if (aggregate.getHints().size() > 0) {
          this.hintsCollect.add("Aggregate:" + aggregate.getHints());
        }
        return super.visit(aggregate);
      }

      @Override public RelNode visit(LogicalCorrelate correlate) {
        if (correlate.getHints().size() > 0) {
          this.hintsCollect.add("Correlate:" + correlate.getHints());
        }
        return super.visit(correlate);
      }

      @Override public RelNode visit(LogicalFilter filter) {
        if (filter.getHints().size() > 0) {
          this.hintsCollect.add("Filter:" + filter.getHints());
        }
        return super.visit(filter);
      }

      @Override public RelNode visit(LogicalUnion union) {
        if (union.getHints().size() > 0) {
          this.hintsCollect.add("Union:" + union.getHints());
        }
        return super.visit(union);
      }

      @Override public RelNode visit(LogicalIntersect intersect) {
        if (intersect.getHints().size() > 0) {
          this.hintsCollect.add("Intersect:" + intersect.getHints());
        }
        return super.visit(intersect);
      }

      @Override public RelNode visit(LogicalMinus minus) {
        if (minus.getHints().size() > 0) {
          this.hintsCollect.add("Minus:" + minus.getHints());
        }
        return super.visit(minus);
      }

      @Override public RelNode visit(LogicalSort sort) {
        if (sort.getHints().size() > 0) {
          this.hintsCollect.add("Sort:" + sort.getHints());
        }
        return super.visit(sort);
      }

      @Override public RelNode visit(LogicalValues values) {
        if (values.getHints().size() > 0) {
          this.hintsCollect.add("Values:" + values.getHints());
        }
        return super.visit(values);
      }

      @Override public RelNode visit(RelNode other) {
        if (other instanceof Window) {
          Window window = (Window) other;
          if (window.getHints().size() > 0) {
            this.hintsCollect.add("Window:" + window.getHints());
          }
        } else if (other instanceof Snapshot) {
          Snapshot snapshot = (Snapshot) other;
          if (snapshot.getHints().size() > 0) {
            this.hintsCollect.add("Snapshot:" + snapshot.getHints());
          }
        }
        return super.visit(other);
      }
    }
  }

  /** Mock appender to collect the logging events. */
  private static class MockAppender {
    final List<String> loggingEvents = new ArrayList<>();

    void append(String event) {
      loggingEvents.add(event);
    }
  }

  /** An utterly useless Logger; a placeholder so that the test compiles and
   * trivially succeeds. */
  private static class MockLogger {
    void addAppender(MockAppender appender) {
    }

    void removeAppender(MockAppender appender) {
    }
  }

  /** Define some tool members and methods for hints test. */
  private static class HintTools {
    //~ Static fields/initializers ---------------------------------------------

    static final String HINT = "properties(k1='v1', k2='v2'), index(ename), no_hash_join";

    static final RelHint PROPS_HINT = RelHint.builder("PROPERTIES")
        .hintOption("K1", "v1")
        .hintOption("K2", "v2")
        .build();

    static final RelHint IDX_HINT = RelHint.builder("INDEX")
        .hintOption("ENAME")
        .build();

    static final RelHint JOIN_HINT = RelHint.builder("NO_HASH_JOIN").build();

    static final HintStrategyTable HINT_STRATEGY_TABLE = createHintStrategies();

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates mock hint strategies.
     *
     * @return HintStrategyTable instance
     */
    private static HintStrategyTable createHintStrategies() {
      return createHintStrategies(HintStrategyTable.builder());
    }

    /**
     * Creates mock hint strategies with given builder.
     *
     * @return HintStrategyTable instance
     */
    static HintStrategyTable createHintStrategies(HintStrategyTable.Builder builder) {
      return builder
        .hintStrategy("no_hash_join", HintPredicates.JOIN)
        .hintStrategy("time_zone", HintPredicates.SET_VAR)
        .hintStrategy("REPARTITION", HintPredicates.SET_VAR)
        .hintStrategy("index", HintPredicates.TABLE_SCAN)
        .hintStrategy("properties", HintPredicates.TABLE_SCAN)
        .hintStrategy(
            "resource", HintPredicates.or(
            HintPredicates.PROJECT, HintPredicates.AGGREGATE,
                HintPredicates.CALC, HintPredicates.VALUES, HintPredicates.FILTER))
        .hintStrategy("AGG_STRATEGY",
            HintStrategy.builder(HintPredicates.AGGREGATE)
                .optionChecker(
                    (hint, errorHandler) -> errorHandler.check(
                    hint.listOptions.size() == 1
                        && (hint.listOptions.get(0).equalsIgnoreCase("ONE_PHASE")
                        || hint.listOptions.get(0).equalsIgnoreCase("TWO_PHASE")),
                    "Hint {} only allows single option, "
                        + "allowed options: [ONE_PHASE, TWO_PHASE]",
                    hint.hintName)).build())
        .hintStrategy("use_hash_join",
          HintPredicates.or(
              HintPredicates.and(HintPredicates.CORRELATE, temporalJoinWithFixedTableName()),
              HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName())))
        .hintStrategy("breakable", HintPredicates.SETOP)
        .hintStrategy("async_merge", HintPredicates.SORT)
        .hintStrategy("mini_batch",
                HintPredicates.and(HintPredicates.WINDOW, HintPredicates.PROJECT))
        .hintStrategy("fast_snapshot", HintPredicates.SNAPSHOT)
        .hintStrategy("use_merge_join",
            HintStrategy.builder(
                HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName()))
                .excludedRules(EnumerableRules.ENUMERABLE_JOIN_RULE).build())
              .hintStrategy(
                  "preserved_project", HintStrategy.builder(
               HintPredicates.PROJECT).excludedRules(CoreRules.FILTER_PROJECT_TRANSPOSE).build())
        .build();
    }

    /** Returns a {@link HintPredicate} for temporal join with specified table references. */
    private static HintPredicate temporalJoinWithFixedTableName() {
      return (hint, rel) -> {
        if (!(rel instanceof LogicalCorrelate)) {
          return false;
        }
        LogicalCorrelate correlate = (LogicalCorrelate) rel;
        Predicate<RelNode> isScan = r -> r instanceof TableScan;
        if (!(isScan.test(correlate.getLeft()))) {
          return false;
        }
        RelNode rightInput = correlate.getRight();
        Predicate<RelNode> isSnapshotOnScan = r -> r instanceof Snapshot
            && isScan.test(((Snapshot) r).getInput());
        RelNode rightScan;
        if (isSnapshotOnScan.test(rightInput)) {
          rightScan = ((Snapshot) rightInput).getInput();
        } else if (rightInput instanceof Filter
            && isSnapshotOnScan.test(((Filter) rightInput).getInput())) {
          rightScan = ((Snapshot) ((Filter) rightInput).getInput()).getInput();
        } else {
          // right child of correlate must be a snapshot on table scan directly or a Filter which
          // input is snapshot on table scan
          return false;
        }
        final List<String> tableNames = hint.listOptions;
        final List<String> inputTables = Stream.of(correlate.getLeft(), rightScan)
            .map(scan -> Util.last(scan.getTable().getQualifiedName()))
            .collect(Collectors.toList());
        return equalsStringList(inputTables, tableNames);
      };
    }

    /** Returns a {@link HintPredicate} for join with specified table references. */
    private static HintPredicate joinWithFixedTableName() {
      return (hint, rel) -> {
        if (!(rel instanceof LogicalJoin)) {
          return false;
        }
        LogicalJoin join = (LogicalJoin) rel;
        final List<String> tableNames = hint.listOptions;
        final List<String> inputTables = join.getInputs().stream()
            .filter(input -> input instanceof TableScan)
            .map(scan -> Util.last(scan.getTable().getQualifiedName()))
            .collect(Collectors.toList());
        return equalsStringList(tableNames, inputTables);
      };
    }

    /** Format the query with hint {@link #HINT}. */
    static String withHint(String sql) {
      return String.format(Locale.ROOT, sql, HINT);
    }
  }
}
