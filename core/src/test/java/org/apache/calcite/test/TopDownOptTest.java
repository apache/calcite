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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

/**
 * Unit test for top-down optimization.
 *
 * <p>As input, the test supplies a SQL statement and rules; the SQL is
 * translated into relational algebra and then fed into a
 * {@link VolcanoPlanner}. The plan before and after "optimization" is
 * diffed against a .ref file using {@link DiffRepository}.
 *
 * <p>Procedure for adding a new test case:
 *
 * <ol>
 * <li>Add a new public test method for your rule, following the existing
 * examples. You'll have to come up with an SQL statement to which your rule
 * will apply in a meaningful way. See
 * {@link org.apache.calcite.test.catalog.MockCatalogReaderSimple} class
 * for details on the schema.
 *
 * <li>Run the test. It should fail. Inspect the output in
 * {@code target/surefire/.../TopDownOptTest.xml}.
 *
 * <li>Verify that the "planBefore" is the correct
 * translation of your SQL, and that it contains the pattern on which your rule
 * is supposed to fire. If all is well, replace
 * {@code src/test/resources/.../TopDownOptTest.xml} and
 * with the new {@code target/surefire/.../TopDownOptTest.xml}.
 *
 * <li>Run the test again. It should fail again, but this time it should contain
 * a "planAfter" entry for your rule. Verify that your rule applied its
 * transformation correctly, and then update the
 * {@code src/test/resources/.../TopDownOptTest.xml} file again.
 *
 * <li>Run the test one last time; this time it should pass.
 * </ol>
 */
class TopDownOptTest extends RelOptTestBase {

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(TopDownOptTest.class);
  }

  Sql sql(String sql) {
    VolcanoPlanner planner = new VolcanoPlanner();
    // Always use top-down optimization
    planner.setTopDownOpt(true);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    RelOptUtil.registerDefaultRules(planner, false, false);

    // Keep deterministic join order
    planner.removeRule(JoinCommuteRule.INSTANCE);
    planner.removeRule(JoinPushThroughJoinRule.LEFT);
    planner.removeRule(JoinPushThroughJoinRule.RIGHT);

    // Always use merge join
    planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    planner.removeRule(EnumerableRules.ENUMERABLE_CALC_RULE);

    // Always use sorted agg
    planner.removeRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);

    Tester tester = createTester().withDecorrelation(true)
        .withClusterFactory(cluster -> RelOptCluster.create(planner, cluster.getRexBuilder()));

    return new Sql(tester, sql, null, planner,
        ImmutableMap.of(), ImmutableList.of());
  }

  @Test void testSortAgg() {
    final String sql = "select mgr, count(*) from sales.emp\n"
        + "group by mgr order by mgr desc nulls last limit 5";
    sql(sql).check();
  }

  @Test void testSortAggPartialKey() {
    final String sql = "select mgr,deptno,comm,count(*) from sales.emp\n"
        + "group by mgr,deptno,comm\n"
        + "order by comm desc nulls last, deptno nulls first";
    sql(sql).check();
  }

  @Test void testSortMergeJoin() {
    final String sql = "select * from\n"
        + "sales.emp r join sales.bonus s on r.ename=s.ename and r.job=s.job\n"
        + "order by r.job desc nulls last, r.ename nulls first";
    sql(sql).check();
  }

  @Test void testSortMergeJoinRight() {
    final String sql = "select * from\n"
        + "sales.emp r join sales.bonus s on r.ename=s.ename and r.job=s.job\n"
        + "order by s.job desc nulls last, s.ename nulls first";
    sql(sql).check();
  }

  @Test void testMergeJoinDeriveLeft1() {
    final String sql = "select * from\n"
        + "(select ename, job, max(sal) from sales.emp group by ename, job) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    sql(sql).check();
  }

  @Test void testMergeJoinDeriveLeft2() {
    final String sql = "select * from\n"
        + "(select ename, job, mgr, max(sal) from sales.emp group by ename, job, mgr) r\n"
        + "join sales.bonus s on r.job=s.job and r.ename=s.ename";
    sql(sql).check();
  }

  @Test void testMergeJoinDeriveRight1() {
    final String sql = "select * from sales.bonus s join\n"
        + "(select ename, job, max(sal) from sales.emp group by ename, job) r\n"
        + "on r.job=s.job and r.ename=s.ename";
    sql(sql).check();
  }

  @Test void testMergeJoinDeriveRight2() {
    final String sql = "select * from sales.bonus s join\n"
        + "(select ename, job, mgr, max(sal) from sales.emp group by ename, job, mgr) r\n"
        + "on r.job=s.job and r.ename=s.ename";
    sql(sql).check();
  }
}
