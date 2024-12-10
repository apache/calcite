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

import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelDotWriter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

import static java.util.Objects.requireNonNull;

/**
 * HepPlannerTest is a unit test for {@link HepPlanner}. See
 * {@link RelOptRulesTest} for an explanation of how to add tests; the tests in
 * this class are targeted at exercising the planner, and use specific rules for
 * convenience only, whereas the tests in that class are targeted at exercising
 * specific rules, and use the planner for convenience only. Hence the split.
 */
class HepPlannerTest {
  //~ Static fields/initializers ---------------------------------------------

  private static final String UNION_TREE =
      "(select name from dept union select ename from emp)"
      + " union (select ename from bonus)";

  private static final String COMPLEX_UNION_TREE = "select * from (\n"
      + "  select ENAME, 50011895 as cat_id, '1' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50011895 union all\n"
      + "  select ENAME, 50013023 as cat_id, '2' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013023 union all\n"
      + "  select ENAME, 50013032 as cat_id, '3' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013032 union all\n"
      + "  select ENAME, 50013024 as cat_id, '4' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013024 union all\n"
      + "  select ENAME, 50004204 as cat_id, '5' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50004204 union all\n"
      + "  select ENAME, 50013043 as cat_id, '6' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013043 union all\n"
      + "  select ENAME, 290903 as cat_id, '7' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 290903 union all\n"
      + "  select ENAME, 50008261 as cat_id, '8' as cat_name, 1 as require_free_postage, 0 as require_15return, 0 as require_48hour,1 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50008261 union all\n"
      + "  select ENAME, 124478013 as cat_id, '9' as cat_name, 0 as require_free_postage, 0 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 124478013 union all\n"
      + "  select ENAME, 124472005 as cat_id, '10' as cat_name, 0 as require_free_postage, 0 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 124472005 union all\n"
      + "  select ENAME, 50013475 as cat_id, '11' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013475 union all\n"
      + "  select ENAME, 50018263 as cat_id, '12' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50018263 union all\n"
      + "  select ENAME, 50013498 as cat_id, '13' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50013498 union all\n"
      + "  select ENAME, 350511 as cat_id, '14' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 350511 union all\n"
      + "  select ENAME, 50019790 as cat_id, '15' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50019790 union all\n"
      + "  select ENAME, 50015382 as cat_id, '16' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50015382 union all\n"
      + "  select ENAME, 350503 as cat_id, '17' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 350503 union all\n"
      + "  select ENAME, 350401 as cat_id, '18' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 350401 union all\n"
      + "  select ENAME, 50015560 as cat_id, '19' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50015560 union all\n"
      + "  select ENAME, 122658003 as cat_id, '20' as cat_name, 0 as require_free_postage, 1 as require_15return, 1 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 122658003 union all\n"
      + "  select ENAME, 50022371 as cat_id, '100' as cat_name, 0 as require_free_postage, 0 as require_15return, 0 as require_48hour,0 as require_insurance from emp where EMPNO = 20171216 and MGR = 0 and ENAME = 'Y' and SAL = 50022371\n"
      + ") a";

  @Nullable
  private static DiffRepository diffRepos = null;

  //~ Methods ----------------------------------------------------------------

  @AfterAll
  public static void checkActualAndReferenceFiles() {
    requireNonNull(diffRepos, "diffRepos").checkActualAndReferenceFiles();
  }

  public RelOptFixture fixture() {
    RelOptFixture fixture = RelOptFixture.DEFAULT
        .withDiffRepos(DiffRepository.lookup(HepPlannerTest.class));
    diffRepos = fixture.diffRepos();
    return fixture;
  }

  /** Sets the SQL statement for a test. */
  public final RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  @Test void testRuleClass() {
    // Verify that an entire class of rules can be applied.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleClass(CoerceInputsRule.class);

    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());

    planner.addRule(
        CoerceInputsRule.Config.DEFAULT
            .withCoerceNames(false)
            .withConsumerRelClass(LogicalUnion.class)
            .toRule());
    planner.addRule(
        CoerceInputsRule.Config.DEFAULT
            .withCoerceNames(false)
            .withConsumerRelClass(LogicalIntersect.class)
            .withDescription("CoerceInputsRule:Intersection") // TODO
            .toRule());

    final String sql = "(select name from dept union select ename from emp)\n"
        + "intersect (select fname from customer.contact)";
    sql(sql).withPlanner(planner).checkUnchanged();
  }

  @Test void testRuleDescription() {
    // Verify that a rule can be applied via its description.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleByDescription("FilterToCalcRule");

    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());

    planner.addRule(CoreRules.FILTER_TO_CALC);

    final String sql = "select name from sales.dept where deptno=12";
    sql(sql).withPlanner(planner).check();
  }

  /**
   * Ensures {@link org.apache.calcite.rel.AbstractRelNode} digest does not include
   * full digest tree.
   */
  @Test void testRelDigestLength() {
    HepProgramBuilder programBuilder = HepProgram.builder();
    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());
    RelNode root = sql(buildUnion(10)).toRel();
    planner.setRoot(root);
    RelNode best = planner.findBestExp();

    // Good digest should look like
    //   rel#66:LogicalProject(input=rel#64:LogicalUnion)
    // Bad digest includes full tree, like
    //   rel#66:LogicalProject(input=rel#64:LogicalUnion(...))
    // So the assertion is to ensure digest includes LogicalUnion exactly once.
    assertIncludesExactlyOnce("best.getDescription()",
        best.toString(), "LogicalUnion");
    assertIncludesExactlyOnce("best.getDigest()",
        best.getDigest(), "LogicalUnion");
  }

  private static String buildUnion(int n) {
    StringBuilder sb = new StringBuilder();
    sb.append("select * from (");
    sb.append("select name from sales.dept");
    for (int i = 0; i < n; i++) {
      sb.append(" union all select name from sales.dept");
    }
    sb.append(")");
    return sb.toString();
  }

  @Test void testPlanToDot() {
    HepProgramBuilder programBuilder = HepProgram.builder();
    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());
    RelNode root = sql("select name from sales.dept").toRel();
    planner.setRoot(root);

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    RelDotWriter planWriter = new RelDotWriter(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
    final RelNode root1 = planner.getRoot();
    assertThat(root1, notNullValue());
    root1.explain(planWriter);
    String planStr = sw.toString();

    assertThat(
        planStr, isLinux("digraph {\n"
            + "\"LogicalTableScan\\ntable = [CATALOG, SA\\nLES, DEPT]\\n\" -> "
            + "\"LogicalProject\\nNAME = $1\\n\" [label=\"0\"]\n"
            + "}\n"));
  }

  private void assertIncludesExactlyOnce(String message, String digest,
      String substring) {
    int pos = 0;
    int cnt = 0;
    while (pos >= 0) {
      pos = digest.indexOf(substring, pos + 1);
      if (pos > 0) {
        cnt++;
      }
    }
    assertThat(message + " should include <<" + substring + ">> exactly once"
        + ", actual value is " + digest,
        cnt, is(1));
  }

  @Test void testMatchLimitOneTopDown() {
    // Verify that only the top union gets rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    programBuilder.addMatchLimit(1);
    programBuilder.addRuleInstance(CoreRules.UNION_TO_DISTINCT);

    sql(UNION_TREE).withProgram(programBuilder.build()).check();
  }

  @Test void testMatchLimitOneBottomUp() {
    // Verify that only the bottom union gets rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchLimit(1);
    programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    programBuilder.addRuleInstance(CoreRules.UNION_TO_DISTINCT);

    sql(UNION_TREE).withProgram(programBuilder.build()).check();
  }

  @Test void testMatchUntilFixpoint() {
    // Verify that both unions get rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT);
    programBuilder.addRuleInstance(CoreRules.UNION_TO_DISTINCT);

    sql(UNION_TREE).withProgram(programBuilder.build()).check();
  }

  @Test void testReplaceCommonSubexpression() {
    // Note that here it may look like the rule is firing
    // twice, but actually it's only firing once on the
    // common sub-expression.  The purpose of this test
    // is to make sure the planner can deal with
    // rewriting something used as a common sub-expression
    // twice by the same parent (the join in this case).

    final String sql = "select d1.deptno from (select * from dept) d1,\n"
        + "(select * from dept) d2";
    sql(sql).withRule(CoreRules.PROJECT_REMOVE).check();
  }

  /** Tests that if two relational expressions are equivalent, the planner
   * notices, and only applies the rule once. */
  @Test void testCommonSubExpression() {
    // In the following,
    //   (select 1 from dept where abs(-1)=20)
    // occurs twice, but it's a common sub-expression, so the rule should only
    // apply once.
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);

    final HepTestListener listener = new HepTestListener(0);
    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.addListener(listener);

    final String sql = "(select 1 from dept where abs(-1)=20)\n"
        + "union all\n"
        + "(select 1 from dept where abs(-1)=20)";
    planner.setRoot(sql(sql).toRel());
    RelNode bestRel = planner.findBestExp();

    assertThat(bestRel.getInput(0).equals(bestRel.getInput(1)), is(true));
    assertThat(listener.getApplyTimes() == 1, is(true));
  }

  @Test void testSubprogram() {
    // Verify that subprogram gets re-executed until fixpoint.
    // In this case, the first time through we limit it to generate
    // only one calc; the second time through it will generate
    // a second calc, and then merge them.
    HepProgramBuilder subprogramBuilder = HepProgram.builder();
    subprogramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    subprogramBuilder.addMatchLimit(1);
    subprogramBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    subprogramBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
    subprogramBuilder.addRuleInstance(CoreRules.CALC_MERGE);

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addSubprogram(subprogramBuilder.build());

    final String sql = "select upper(ename) from\n"
        + "(select lower(ename) as ename from emp where empno = 100)";
    sql(sql).withProgram(programBuilder.build()).check();
  }

  @Test void testGroup() {
    // Verify simultaneous application of a group of rules.
    // Intentionally add them in the wrong order to make sure
    // that order doesn't matter within the group.
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addGroupBegin();
    programBuilder.addRuleInstance(CoreRules.CALC_MERGE);
    programBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    programBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);
    programBuilder.addGroupEnd();

    final String sql = "select upper(name) from dept where deptno=20";
    sql(sql).withProgram(programBuilder.build()).check();
  }

  @Test void testGC() {
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    programBuilder.addRuleInstance(CoreRules.CALC_MERGE);
    programBuilder.addRuleInstance(CoreRules.PROJECT_TO_CALC);
    programBuilder.addRuleInstance(CoreRules.FILTER_TO_CALC);

    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(
        sql("select upper(name) from dept where deptno=20").toRel());
    planner.findBestExp();
    // Reuse of HepPlanner (should trigger GC).
    planner.setRoot(
        sql("select upper(name) from dept where deptno=20").toRel());
    planner.findBestExp();
  }

  @Test void testRelNodeCacheWithDigest() {
    HepProgramBuilder programBuilder = HepProgram.builder();
    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());
    String query = "(select n_nationkey from SALES.CUSTOMER) union all\n"
        + "(select n_name from CUSTOMER_MODIFIABLEVIEW)";
    sql(query)
        .withDynamicTable()
        .withDecorrelate(true)
        .withProgram(programBuilder.build())
        .withPlanner(planner)
        .checkUnchanged();
  }

  @Test void testRuleApplyCount() {
    final long applyTimes1 = checkRuleApplyCount(HepMatchOrder.ARBITRARY);
    assertThat(applyTimes1, is(316L));

    final long applyTimes2 = checkRuleApplyCount(HepMatchOrder.DEPTH_FIRST);
    assertThat(applyTimes2, is(87L));
  }

  @Test void testMaterialization() {
    HepPlanner planner = new HepPlanner(HepProgram.builder().build());
    RelNode tableRel = sql("select * from dept").toRel();
    RelNode queryRel = tableRel;
    RelOptMaterialization mat1 =
        new RelOptMaterialization(tableRel, queryRel, null,
            ImmutableList.of("default", "mv"));
    planner.addMaterialization(mat1);
    assertThat(planner.getMaterializations(), hasSize(1));
    assertThat(mat1, is(planner.getMaterializations().get(0)));
    planner.clear();
    assertThat(planner.getMaterializations(), empty());
  }

  private long checkRuleApplyCount(HepMatchOrder matchOrder) {
    final HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(matchOrder);
    programBuilder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    programBuilder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final HepTestListener listener = new HepTestListener(0);
    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.addListener(listener);
    planner.setRoot(sql(COMPLEX_UNION_TREE).toRel());
    planner.findBestExp();
    return listener.getApplyTimes();
  }

  /** Listener for HepPlannerTest; counts how many times rules fire. */
  private static class HepTestListener implements RelOptListener {
    private long applyTimes;

    HepTestListener(long applyTimes) {
      this.applyTimes = applyTimes;
    }

    long getApplyTimes() {
      return applyTimes;
    }

    @Override public void relEquivalenceFound(RelEquivalenceEvent event) {
    }

    @Override public void ruleAttempted(RuleAttemptedEvent event) {
      if (event.isBefore()) {
        ++applyTimes;
      }
    }

    @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
    }

    @Override public void relDiscarded(RelDiscardedEvent event) {
    }

    @Override public void relChosen(RelChosenEvent event) {
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5401">[CALCITE-5401]
   * Rule fired by HepPlanner can return Volcano's RelSubset</a>. */
  @Test void testAggregateRemove() {
    final RelBuilder builder = RelBuilderTest.createBuilder(c -> c.withAggregateUnique(true));
    final RelNode root =
        builder
            .values(new String[]{"i"}, 1, 2, 3)
            .distinct()
            .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REMOVE)
        .build();
    final HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root);
    final RelNode result = planner.findBestExp();
    assertThat(result, is(instanceOf(LogicalValues.class)));
  }
}
