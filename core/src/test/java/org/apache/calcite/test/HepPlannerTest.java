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

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;

import org.junit.Test;

/**
 * HepPlannerTest is a unit test for {@link HepPlanner}. See
 * {@link RelOptRulesTest} for an explanation of how to add tests; the tests in
 * this class are targeted at exercising the planner, and use specific rules for
 * convenience only, whereas the tests in that class are targeted at exercising
 * specific rules, and use the planner for convenience only. Hence the split.
 */
public class HepPlannerTest extends RelOptTestBase {
  //~ Static fields/initializers ---------------------------------------------

  private static final String UNION_TREE =
      "(select name from dept union select ename from emp)"
      + " union (select ename from bonus)";

  //~ Methods ----------------------------------------------------------------

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(HepPlannerTest.class);
  }

  @Test public void testRuleClass() throws Exception {
    // Verify that an entire class of rules can be applied.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleClass(CoerceInputsRule.class);

    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());

    planner.addRule(
        new CoerceInputsRule(LogicalUnion.class, false, RelFactories.LOGICAL_BUILDER));
    planner.addRule(
        new CoerceInputsRule(LogicalIntersect.class, false, RelFactories.LOGICAL_BUILDER));

    checkPlanning(planner,
        "(select name from dept union select ename from emp)"
            + " intersect (select fname from customer.contact)");
  }

  @Test public void testRuleDescription() throws Exception {
    // Verify that a rule can be applied via its description.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleByDescription("FilterToCalcRule");

    HepPlanner planner =
        new HepPlanner(
            programBuilder.build());

    planner.addRule(FilterToCalcRule.INSTANCE);

    checkPlanning(
        planner,
        "select name from sales.dept where deptno=12");
  }

  @Test public void testMatchLimitOneTopDown() throws Exception {
    // Verify that only the top union gets rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    programBuilder.addMatchLimit(1);
    programBuilder.addRuleInstance(UnionToDistinctRule.INSTANCE);

    checkPlanning(
        programBuilder.build(), UNION_TREE);
  }

  @Test public void testMatchLimitOneBottomUp() throws Exception {
    // Verify that only the bottom union gets rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchLimit(1);
    programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    programBuilder.addRuleInstance(UnionToDistinctRule.INSTANCE);

    checkPlanning(
        programBuilder.build(), UNION_TREE);
  }

  @Test public void testMatchUntilFixpoint() throws Exception {
    // Verify that both unions get rewritten.

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT);
    programBuilder.addRuleInstance(UnionToDistinctRule.INSTANCE);

    checkPlanning(
        programBuilder.build(), UNION_TREE);
  }

  @Test public void testReplaceCommonSubexpression() throws Exception {
    // Note that here it may look like the rule is firing
    // twice, but actually it's only firing once on the
    // common sub-expression.  The purpose of this test
    // is to make sure the planner can deal with
    // rewriting something used as a common sub-expression
    // twice by the same parent (the join in this case).

    checkPlanning(
        ProjectRemoveRule.INSTANCE,
        "select d1.deptno from (select * from dept) d1,"
            + " (select * from dept) d2");
  }

  @Test public void testSubprogram() throws Exception {
    // Verify that subprogram gets re-executed until fixpoint.
    // In this case, the first time through we limit it to generate
    // only one calc; the second time through it will generate
    // a second calc, and then merge them.
    HepProgramBuilder subprogramBuilder = HepProgram.builder();
    subprogramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    subprogramBuilder.addMatchLimit(1);
    subprogramBuilder.addRuleInstance(ProjectToCalcRule.INSTANCE);
    subprogramBuilder.addRuleInstance(CalcMergeRule.INSTANCE);

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addSubprogram(subprogramBuilder.build());

    checkPlanning(
        programBuilder.build(),
        "select upper(ename) from (select lower(ename) as ename from emp)");
  }

  @Test public void testGroup() throws Exception {
    // Verify simultaneous application of a group of rules.
    // Intentionally add them in the wrong order to make sure
    // that order doesn't matter within the group.
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addGroupBegin();
    programBuilder.addRuleInstance(CalcMergeRule.INSTANCE);
    programBuilder.addRuleInstance(ProjectToCalcRule.INSTANCE);
    programBuilder.addRuleInstance(FilterToCalcRule.INSTANCE);
    programBuilder.addGroupEnd();

    checkPlanning(
        programBuilder.build(),
        "select upper(name) from dept where deptno=20");
  }

  @Test public void testGC() throws Exception {
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    programBuilder.addRuleInstance(CalcMergeRule.INSTANCE);
    programBuilder.addRuleInstance(ProjectToCalcRule.INSTANCE);
    programBuilder.addRuleInstance(FilterToCalcRule.INSTANCE);

    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(
        tester.convertSqlToRel("select upper(name) from dept where deptno=20").rel);
    planner.findBestExp();
    // Reuse of HepPlanner (should trigger GC).
    planner.setRoot(
        tester.convertSqlToRel("select upper(name) from dept where deptno=20").rel);
    planner.findBestExp();
  }
}

// End HepPlannerTest.java
