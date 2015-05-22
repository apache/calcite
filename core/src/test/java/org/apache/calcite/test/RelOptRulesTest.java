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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateFilterTransposeRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinAddRedundantSemiJoinRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinExtractFilterRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.JoinUnionTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinJoinTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for rules in {@code org.apache.calcite.rel} and subpackages.
 *
 * <p>As input, the test supplies a SQL statement and a single rule; the SQL is
 * translated into relational algebra and then fed into a
 * {@link org.apache.calcite.plan.hep.HepPlanner}. The planner fires the rule on
 * every
 * pattern match in a depth-first left-to-right preorder traversal of the tree
 * for as long as the rule continues to succeed in applying its transform. (For
 * rules which call transformTo more than once, only the last result is used.)
 * The plan before and after "optimization" is diffed against a .ref file using
 * {@link DiffRepository}.
 *
 * <p>Procedure for adding a new test case:
 *
 * <ol>
 * <li>Add a new public test method for your rule, following the existing
 * examples. You'll have to come up with an SQL statement to which your rule
 * will apply in a meaningful way. See {@link SqlToRelTestBase} class comments
 * for details on the schema.
 *
 * <li>Run the test. It should fail. Inspect the output in
 * {@code target/surefire/.../RelOptRulesTest.xml}.
 * (If you are running using maven and this file does not exist, add a
 * {@code -X} flag to the maven command line.)
 *
 * <li>Verify that the "planBefore" is the correct
 * translation of your SQL, and that it contains the pattern on which your rule
 * is supposed to fire. If all is well, replace
 * {@code src/test/resources/.../RelOptRulesTest.xml} and
 * with the new {@code target/surefire/.../RelOptRulesTest.xml}.
 *
 * <li>Run the test again. It should fail again, but this time it should contain
 * a "planAfter" entry for your rule. Verify that your rule applied its
 * transformation correctly, and then update the
 * {@code src/test/resources/.../RelOptRulesTest.xml} file again.
 *
 * <li>Run the test one last time; this time it should pass.
 * </ol>
 */
public class RelOptRulesTest extends RelOptTestBase {
  //~ Methods ----------------------------------------------------------------

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(RelOptRulesTest.class);
  }


  @Test public void testUnionToDistinctRule() {
    checkPlanning(UnionToDistinctRule.INSTANCE,
        "select * from dept union select * from dept");
  }

  @Test public void testExtractJoinFilterRule() {
    checkPlanning(JoinExtractFilterRule.INSTANCE,
        "select 1 from emp inner join dept on emp.deptno=dept.deptno");
  }

  @Test public void testAddRedundantSemiJoinRule() {
    checkPlanning(JoinAddRedundantSemiJoinRule.INSTANCE,
        "select 1 from emp inner join dept on emp.deptno = dept.deptno");
  }

  @Test public void testStrengthenJoinType() {
    // The "Filter(... , right.c IS NOT NULL)" above a left join is pushed into
    // the join, makes it an inner join, and then disappears because c is NOT
    // NULL.
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .build();
    checkPlanning(tester.withDecorrelation(true).withTrim(true), preProgram,
        new HepPlanner(program),
        "select * from dept where exists (\n"
            + "  select * from emp\n"
            + "  where emp.deptno = dept.deptno\n"
            + "  and emp.sal > 100)");
  }

  @Test public void testFullOuterJoinSimplificationToLeftOuter() {
    checkPlanning(FilterJoinRule.FILTER_ON_JOIN,
        "select 1 from sales.dept d full outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where d.name = 'Charlie'");
  }

  @Test public void testFullOuterJoinSimplificationToRightOuter() {
    checkPlanning(FilterJoinRule.FILTER_ON_JOIN,
        "select 1 from sales.dept d full outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where e.sal > 100");
  }

  @Test public void testFullOuterJoinSimplificationToInner() {
    checkPlanning(FilterJoinRule.FILTER_ON_JOIN,
        "select 1 from sales.dept d full outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where d.name = 'Charlie' and e.sal > 100");
  }

  @Test public void testLeftOuterJoinSimplificationToInner() {
    checkPlanning(FilterJoinRule.FILTER_ON_JOIN,
        "select 1 from sales.dept d left outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where e.sal > 100");
  }


  @Test public void testRightOuterJoinSimplificationToInner() {
    checkPlanning(FilterJoinRule.FILTER_ON_JOIN,
        "select 1 from sales.dept d right outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where d.name = 'Charlie'");
  }

  @Test public void testPushFilterPastAgg() {
    checkPlanning(FilterAggregateTransposeRule.INSTANCE,
        "select dname, c from"
            + " (select name dname, count(*) as c from dept group by name) t"
            + " where dname = 'Charlie'");
  }

  private void basePushFilterPastAggWithGroupingSets() throws Exception {
    final HepProgram preProgram =
            HepProgram.builder()
                .addRuleInstance(ProjectMergeRule.INSTANCE)
                .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
                .build();
    final HepProgram program =
            HepProgram.builder()
                .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
                .build();
    checkPlanning(tester, preProgram, new HepPlanner(program), "${sql}");
  }

  @Test public void testPushFilterPastAggWithGroupingSets1() throws Exception {
    basePushFilterPastAggWithGroupingSets();
  }

  @Test public void testPushFilterPastAggWithGroupingSets2() throws Exception {
    basePushFilterPastAggWithGroupingSets();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-434">[CALCITE-434],
   * FilterAggregateTransposeRule loses conditions that cannot be pushed</a>. */
  @Test public void testPushFilterPastAggTwo() {
    checkPlanning(FilterAggregateTransposeRule.INSTANCE,
        "select dept1.c1 from (\n"
            + "  select dept.name as c1, count(*) as c2\n"
            + "  from dept where dept.name > 'b' group by dept.name) dept1\n"
            + "where dept1.c1 > 'c' and (dept1.c2 > 30 or dept1.c1 < 'z')");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-448">[CALCITE-448],
   * FilterIntoJoinRule creates filters containing invalid RexInputRef</a>. */
  @Test public void testPushFilterPastProject() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final FilterJoinRule.Predicate predicate =
        new FilterJoinRule.Predicate() {
          public boolean apply(Join join, JoinRelType joinType, RexNode exp) {
            return joinType != JoinRelType.INNER;
          }
        };
    final FilterJoinRule join =
        new FilterJoinRule.JoinConditionPushRule(
            RelFactories.DEFAULT_FILTER_FACTORY,
            RelFactories.DEFAULT_PROJECT_FACTORY, predicate);
    final FilterJoinRule filterOnJoin =
        new FilterJoinRule.FilterIntoJoinRule(true,
            RelFactories.DEFAULT_FILTER_FACTORY,
            RelFactories.DEFAULT_PROJECT_FACTORY, predicate);
    final HepProgram program =
        HepProgram.builder()
            .addGroupBegin()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(join)
            .addRuleInstance(filterOnJoin)
            .addGroupEnd()
            .build();
    checkPlanning(tester,
        preProgram,
        new HepPlanner(program),
        "select a.name\n"
            + "from dept a\n"
            + "left join dept b on b.deptno > 10\n"
            + "right join dept c on b.deptno > 10\n");
  }

  @Test public void testSemiJoinRule() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(SemiJoinRule.INSTANCE)
            .build();
    checkPlanning(tester.withDecorrelation(true).withTrim(true), preProgram,
        new HepPlanner(program),
        "select * from dept where exists (\n"
            + "  select * from emp\n"
            + "  where emp.deptno = dept.deptno\n"
            + "  and emp.sal > 100)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-438">[CALCITE-438],
   * Push predicates through SemiJoin</a>. */
  @Test public void testPushFilterThroughSemiJoin() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(SemiJoinRule.INSTANCE)
            .build();

    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(FilterJoinRule.JOIN)
            .build();
    checkPlanning(tester.withDecorrelation(true).withTrim(false), preProgram,
        new HepPlanner(program),
        "select * from (select * from dept where dept.deptno in (\n"
            + "  select emp.deptno from emp\n"
            + "  ))R where R.deptno <=10 ");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-571">[CALCITE-571],
   * ReduceExpressionsRule tries to reduce SemiJoin condition to non-equi
   * condition</a>. */
  @Test public void testSemiJoinReduceConstants() {
    final HepProgram preProgram = HepProgram.builder().addRuleInstance(
        SemiJoinRule.INSTANCE)
            .build();

    final HepProgram program = HepProgram.builder().addRuleInstance(
        ReduceExpressionsRule.JOIN_INSTANCE)
            .build();
    checkPlanning(tester.withDecorrelation(false).withTrim(true), preProgram,
        new HepPlanner(program),
        "select e1.sal from (select * from emp where deptno = 200) as e1\n"
            + "where e1.deptno in (\n"
            + "  select e2.deptno from emp e2 where e2.sal = 100)");
  }

  protected void semiJoinTrim() {
    final DiffRepository diffRepos = getDiffRepos();
    String sql = diffRepos.expand(null, "${sql}");

    TesterImpl t = (TesterImpl) tester;
    final RelDataTypeFactory typeFactory = t.getTypeFactory();
    final Prepare.CatalogReader catalogReader =
        t.createCatalogReader(typeFactory);
    final SqlValidator validator =
        t.createValidator(
            catalogReader, typeFactory);
    final SqlToRelConverter converter =
        t.createSqlToRelConverter(
            validator,
            catalogReader,
            typeFactory);

    final SqlNode sqlQuery;
    try {
      sqlQuery = t.parseQuery(sql);
    } catch (Exception e) {
      throw Util.newInternal(e);
    }

    final SqlNode validatedQuery = validator.validate(sqlQuery);
    RelNode rel =
        converter.convertQuery(validatedQuery, false, true);
    rel = converter.decorrelate(sqlQuery, rel);

    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .addRuleInstance(SemiJoinRule.INSTANCE)
            .build();

    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(rel);
    rel = planner.findBestExp();

    String planBefore = NL + RelOptUtil.toString(rel);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    converter.setTrimUnusedFields(true);
    rel = converter.trimUnusedFields(rel);
    String planAfter = NL + RelOptUtil.toString(rel);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testSemiJoinTrim() {
    semiJoinTrim();
  }

  @Test public void testReduceAverage() {
    checkPlanning(AggregateReduceFunctionsRule.INSTANCE,
        "select name, max(name), avg(deptno), min(name)"
            + " from sales.dept group by name");
  }

  @Test public void testDistinctCount1() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, count(distinct ename)"
            + " from sales.emp group by deptno");
  }

  @Test public void testDistinctCount2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, count(distinct ename), sum(sal)"
            + " from sales.emp group by deptno");
  }

  /** Tests implementing multiple distinct count the old way, using a join. */
  @Test public void testDistinctCountMultipleViaJoin() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, count(distinct ename), count(distinct job, ename),\n"
            + "  count(distinct deptno, job), sum(sal)\n"
            + " from sales.emp group by deptno");
  }

  /** Tests implementing multiple distinct count the new way, using GROUPING
   *  SETS. */
  @Test public void testDistinctCountMultiple() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, count(distinct ename), count(distinct job)\n"
            + " from sales.emp group by deptno");
  }

  @Test public void testDistinctCountMultipleNoGroup() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select count(distinct ename), count(distinct job)\n"
            + " from sales.emp");
  }

  @Test public void testDistinctCountMixedJoin() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, count(distinct ename), count(distinct job, ename),\n"
            + "  count(distinct deptno, job), sum(sal)\n"
            + " from sales.emp group by deptno");
  }

  @Test public void testDistinctCountMixed() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, count(distinct deptno, job) as cddj, sum(sal) as s\n"
            + " from sales.emp group by deptno");
  }

  @Test public void testDistinctCountMixed2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, count(distinct ename) as cde,\n"
            + "  count(distinct job, ename) as cdje,\n"
            + "  count(distinct deptno, job) as cddj,\n"
            + "  sum(sal) as s\n"
            + " from sales.emp group by deptno");
  }

  @Test public void testDistinctCountGroupingSets1() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, job, count(distinct ename)"
            + " from sales.emp group by rollup(deptno,job)");
  }

  @Test public void testDistinctCountGroupingSets2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select deptno, job, count(distinct ename), sum(sal)"
            + " from sales.emp group by rollup(deptno,job)");
  }

  @Test public void testPushProjectPastFilter() {
    checkPlanning(ProjectFilterTransposeRule.INSTANCE,
        "select empno + deptno from emp where sal = 10 * comm "
            + "and upper(ename) = 'FOO'");
  }

  @Test public void testPushProjectPastJoin() {
    checkPlanning(ProjectJoinTransposeRule.INSTANCE,
        "select e.sal + b.comm from emp e inner join bonus b "
            + "on e.ename = b.ename and e.deptno = 10");
  }

  @Test public void testPushProjectPastSetOp() {
    checkPlanning(ProjectSetOpTransposeRule.INSTANCE,
        "select sal from "
            + "(select * from emp e1 union all select * from emp e2)");
  }

  @Test public void testPushJoinThroughUnionOnLeft() {
    checkPlanning(JoinUnionTransposeRule.LEFT_UNION,
        "select r1.sal from "
            + "(select * from emp e1 union all select * from emp e2) r1, "
            + "emp r2");
  }

  @Test public void testPushJoinThroughUnionOnRight() {
    checkPlanning(JoinUnionTransposeRule.RIGHT_UNION,
        "select r1.sal from "
            + "emp r1, "
            + "(select * from emp e1 union all select * from emp e2) r2");
  }

  @Ignore("cycles")
  @Test public void testMergeFilterWithJoinCondition() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(TableScanRule.INSTANCE)
        .addRuleInstance(JoinExtractFilterRule.INSTANCE)
        .addRuleInstance(FilterToCalcRule.INSTANCE)
        .addRuleInstance(CalcMergeRule.INSTANCE)
            //.addRuleInstance(FennelCalcRule.instance);
            //.addRuleInstance(FennelCartesianJoinRule.instance);
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .build();

    checkPlanning(program,
        "select d.name as dname,e.ename as ename"
            + " from emp e inner join dept d"
            + " on e.deptno=d.deptno"
            + " where d.name='Propane'");
  }

  /** Tests that filters are combined if they are identical. */
  @Test public void testMergeFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterMergeRule.INSTANCE)
        .build();

    checkPlanning(program,
        "select name from (\n"
            + "  select *\n"
            + "  from dept\n"
            + "  where deptno = 10)\n"
            + "where deptno = 10\n");
  }

  /** Tests that a filters is combined are combined if they are identical,
   * even if one of them originates in an ON clause of a JOIN. */
  @Test public void testMergeJoinFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterMergeRule.INSTANCE)
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .build();

    checkPlanning(program,
        "select * from (\n"
            + "  select d.deptno, e.ename\n"
            + "  from emp as e\n"
            + "  join dept as d\n"
            + "  on e.deptno = d.deptno\n"
            + "  and d.deptno = 10)\n"
            + "where deptno = 10\n");
  }

  @Ignore("cycles")
  @Test public void testHeterogeneousConversion() throws Exception {
    // This one tests the planner's ability to correctly
    // apply different converters on top of a common
    // sub-expression.  The common sub-expression is the
    // reference to the table sales.emps.  On top of that
    // are two projections, unioned at the top.  For one
    // of the projections, we force a Fennel implementation.
    // For the other, we force a Java implementation.
    // Then, we request conversion from Fennel to Java,
    // and verify that it only applies to one usage of the
    // table, not both (which would be incorrect).
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(TableScanRule.INSTANCE)
        .addRuleInstance(ProjectToCalcRule.INSTANCE)

            // Control the calc conversion.
        .addMatchLimit(1)

            // Let the converter rule fire to its heart's content.
        .addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT)
        .build();

    checkPlanning(program,
        "select upper(ename) from emp union all"
            + " select lower(ename) from emp");
  }

  @Test public void testPushSemiJoinPastJoinRuleLeft() throws Exception {
    // tests the case where the semijoin is pushed to the left
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinAddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(SemiJoinJoinTransposeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and e1.empno = e2.empno");
  }

  @Test public void testPushSemiJoinPastJoinRuleRight() throws Exception {
    // tests the case where the semijoin is pushed to the right
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinAddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(SemiJoinJoinTransposeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Test public void testPushSemiJoinPastFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinAddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(SemiJoinFilterTransposeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
            + "where e.deptno = d.deptno and e.ename = 'foo'");
  }

  @Test public void testConvertMultiJoinRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Test public void testReduceConstants() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    // NOTE jvs 27-May-2006: among other things, this verifies
    // intentionally different treatment for identical coalesce expression
    // in select and where.

    // There is "CAST(2 AS INTEGER)" in the plan because 2 has type "INTEGER NOT
    // NULL" and we need "INTEGER".
    checkPlanning(program,
        "select 1+2, d.deptno+(3+4), (5+6)+d.deptno, cast(null as integer),"
            + " coalesce(2,null), row(7+8)"
            + " from dept d inner join emp e"
            + " on d.deptno = e.deptno + (5-5)"
            + " where d.deptno=(7+8) and d.deptno=(8+7) and d.deptno=coalesce(2,null)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-570">[CALCITE-570],
   * ReduceExpressionsRule throws "duplicate key" exception</a>. */
  @Test public void testReduceConstantsDup() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    checkPlanning(program,
        "select d.deptno"
            + " from dept d"
            + " where d.deptno=7 and d.deptno=8");
  }

  @Test public void testReduceConstants2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    checkPlanning(program,
        "select p1 is not distinct from p0 from (values (2, cast(null as integer))) as t(p0, p1)");
  }

  @Test public void testReduceConstantsEliminatesFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    // WHERE NULL is the same as WHERE FALSE, so get empty result
    checkPlanning(program,
        "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-566">[CALCITE-566],
   * ReduceExpressionsRule requires planner to have an Executor</a>. */
  @Test public void testReduceConstantsRequiresExecutor() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    // Remove the executor
    tester.convertSqlToRel("values 1").getCluster().getPlanner()
        .setExecutor(null);

    // Rule should not fire, but there should be no NPE
    checkPlanning(program,
        "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)");
  }

  @Test public void testAlreadyFalseEliminatesFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    checkPlanning(program,
        "select * from (values (1,2)) where false");
  }

  @Test public void testReduceConstantsCalc() throws Exception {
    // This reduction does not work using
    // ReduceExpressionsRule.PROJECT_INSTANCE or FILTER_INSTANCE,
    // only CALC_INSTANCE, because we need to pull the project expression
    //    upper('table')
    // into the condition
    //    upper('table') = 'TABLE'
    // and reduce it to TRUE. Only in the Calc are projects and conditions
    // combined.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterSetOpTransposeRule.INSTANCE)
        .addRuleInstance(FilterToCalcRule.INSTANCE)
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .addRuleInstance(CalcMergeRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)

            // the hard part is done... a few more rule calls to clean up
        .addRuleInstance(PruneEmptyRules.UNION_INSTANCE)
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .addRuleInstance(CalcMergeRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)
        .build();

    // Result should be same as typing
    //  SELECT * FROM (VALUES ('TABLE        ', 'T')) AS T(U, S)
    checkPlanning(program,
        "select * from (\n"
            + "  select upper(substring(x FROM 1 FOR 2) || substring(x FROM 3)) as u,\n"
            + "      substring(x FROM 1 FOR 1) as s\n"
            + "  from (\n"
            + "    select 'table' as x from (values (true))\n"
            + "    union\n"
            + "    select 'view' from (values (true))\n"
            + "    union\n"
            + "    select 'foreign table' from (values (true))\n"
            + "  )\n"
            + ") where u = 'TABLE'");
  }

  @Test public void testRemoveSemiJoin() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinAddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(SemiJoinRemoveRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
            + "where e.deptno = d.deptno");
  }

  @Test public void testRemoveSemiJoinWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinAddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(SemiJoinFilterTransposeRule.INSTANCE)
        .addRuleInstance(SemiJoinRemoveRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e.ename from emp e, dept d "
            + "where e.deptno = d.deptno and e.ename = 'foo'");
  }

  @Test public void testRemoveSemiJoinRight() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinAddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(SemiJoinJoinTransposeRule.INSTANCE)
        .addRuleInstance(SemiJoinRemoveRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
  }

  @Test public void testRemoveSemiJoinRightWithFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinAddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(SemiJoinJoinTransposeRule.INSTANCE)
        .addRuleInstance(SemiJoinFilterTransposeRule.INSTANCE)
        .addRuleInstance(SemiJoinRemoveRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e1.ename from emp e1, dept d, emp e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno "
            + "and d.name = 'foo'");
  }

  private void checkPlanning(String query) throws Exception {
    final Tester tester1 = tester.withCatalogReaderFactory(
        new Function<RelDataTypeFactory, Prepare.CatalogReader>() {
          public Prepare.CatalogReader apply(RelDataTypeFactory typeFactory) {
            return new MockCatalogReader(typeFactory, true) {
              @Override public MockCatalogReader init() {
                // CREATE SCHEMA abc;
                // CREATE TABLE a(a INT);
                // ...
                // CREATE TABLE j(j INT);
                MockSchema schema = new MockSchema("SALES");
                registerSchema(schema);
                final RelDataType intType =
                    typeFactory.createSqlType(SqlTypeName.INTEGER);
                for (int i = 0; i < 10; i++) {
                  String t = String.valueOf((char) ('A' + i));
                  MockTable table = MockTable.create(this, schema, t, false);
                  table.addColumn(t, intType);
                  registerTable(table);
                }
                return this;
              }
              // CHECKSTYLE: IGNORE 1
            }.init();
          }
        });
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(ProjectRemoveRule.INSTANCE)
        .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
        .build();
    checkPlanning(tester1, null,
        new HepPlanner(program), query);
  }

  @Test public void testConvertMultiJoinRuleOuterJoins() throws Exception {
    checkPlanning("select * from "
        + "    (select * from "
        + "        (select * from "
        + "            (select * from A right outer join B on a = b) "
        + "            left outer join "
        + "            (select * from C full outer join D on c = d)"
        + "            on a = c and b = d) "
        + "        right outer join "
        + "        (select * from "
        + "            (select * from E full outer join F on e = f) "
        + "            right outer join "
        + "            (select * from G left outer join H on g = h) "
        + "            on e = g and f = h) "
        + "        on a = e and b = f and c = g and d = h) "
        + "    inner join "
        + "    (select * from I inner join J on i = j) "
        + "    on a = i and h = j");
  }

  @Test public void testConvertMultiJoinRuleOuterJoins2() throws Exception {
    // in (A right join B) join C, pushing C is not allowed;
    // therefore there should be 2 MultiJoin
    checkPlanning("select * from A right join B on a = b join C on b = c");
  }

  @Test public void testConvertMultiJoinRuleOuterJoins3() throws Exception {
    // in (A join B) left join C, pushing C is allowed;
    // therefore there should be 1 MultiJoin
    checkPlanning("select * from A join B on a = b left join C on b = c");
  }

  @Test public void testConvertMultiJoinRuleOuterJoins4() throws Exception {
    // in (A join B) right join C, pushing C is not allowed;
    // therefore there should be 2 MultiJoin
    checkPlanning("select * from A join B on a = b right join C on b = c");
  }

  @Test public void testPushSemiJoinPastProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinAddRedundantSemiJoinRule.INSTANCE)
        .addRuleInstance(SemiJoinProjectTransposeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select e.* from "
            + "(select ename, trim(job), sal * 2, deptno from emp) e, dept d "
            + "where e.deptno = d.deptno");
  }

  @Test public void testReduceValuesUnderFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(ValuesReduceRule.FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select a, b from (values (10,'x')) as t(a, b)");
    checkPlanning(program,
        "select a, b from (values (10, 'x'), (20, 'y')) as t(a, b) where a < 15");
  }

  @Test public void testReduceValuesUnderProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ValuesReduceRule.PROJECT_INSTANCE)
        .build();

    // Plan should be same as for
    // select a, b as x from (values (11), (23)) as t(x)");
    checkPlanning(program,
        "select a + b from (values (10, 1), (20, 3)) as t(a, b)");
  }

  @Test public void testReduceValuesUnderProjectFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    checkPlanning(program,
        "select a + b as x, b, a from (values (10, 1), (30, 7), (20, 3)) as t(a, b)"
            + " where a - b < 21");
  }

  @Ignore // Calcite does not support INSERT yet
  @Test public void testReduceValuesNull() throws Exception {
    // The NULL literal presents pitfalls for value-reduction. Only
    // an INSERT statement contains un-CASTed NULL values.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ValuesReduceRule.PROJECT_INSTANCE)
        .build();
    checkPlanning(program,
        "insert into sales.depts(deptno,name) values (NULL, 'null')");
  }

  @Test public void testReduceValuesToEmpty() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    checkPlanning(program,
        "select a + b as x, b, a from (values (10, 1), (30, 7)) as t(a, b)"
            + " where a - b < 0");
  }

  @Test public void testEmptyFilterProjectUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterSetOpTransposeRule.INSTANCE)
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.UNION_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (30, 3)) as t(x, y)");
    checkPlanning(program,
        "select * from (\n"
            + "select * from (values (10, 1), (30, 3)) as t (x, y)\n"
            + "union all\n"
            + "select * from (values (20, 2))\n"
            + ")\n"
            + "where x + y > 30");
  }

  @Test public void testEmptyJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    checkPlanning(program,
        "select * from (\n"
            + "select * from emp where false)\n"
            + "join dept using (deptno)");
  }

  @Test public void testEmptyJoinLeft() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    checkPlanning(program,
        "select * from (\n"
            + "select * from emp where false)\n"
            + "left join dept using (deptno)");
  }

  @Test public void testEmptyJoinRight() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be equivalent to "select * from emp join dept".
    // Cannot optimize away the join because of RIGHT.
    checkPlanning(program,
        "select * from (\n"
            + "select * from emp where false)\n"
            + "right join dept using (deptno)");
  }

  @Test public void testEmptySort() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.SORT_INSTANCE)
        .build();

    checkPlanning(program,
        "select * from emp where false order by deptno");
  }

  @Test public void testEmptySortLimitZero() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE)
        .build();

    checkPlanning(program,
        "select * from emp order by deptno limit 0");
  }

  @Test public void testReduceCasts() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    // The resulting plan should have no cast expressions
    checkPlanning(program,
        "select cast(d.name as varchar(128)), cast(e.empno as integer) "
            + "from dept as d inner join emp as e "
            + "on cast(d.deptno as integer) = cast(e.deptno as integer) "
            + "where cast(e.job as varchar(1)) = 'Manager'");
  }

  @Test public void testReduceCastAndConsts() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    // Make sure constant expressions inside the cast can be reduced
    // in addition to the casts.
    checkPlanning(program,
        "select * from emp "
            + "where cast((empno + (10/2)) as int) = 13");
  }

  @Ignore // Calcite does not support INSERT yet
  @Test public void testReduceCastsNullable() throws Exception {
    HepProgram program = new HepProgramBuilder()

        // Simulate the way INSERT will insert casts to the target types
        .addRuleInstance(
            new CoerceInputsRule(LogicalTableModify.class, false))

            // Convert projects to calcs, merge two calcs, and then
            // reduce redundant casts in merged calc.
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .addRuleInstance(CalcMergeRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.CALC_INSTANCE)
        .build();
    checkPlanning(program,
        "insert into sales.depts(name) "
            + "select cast(gender as varchar(128)) from sales.emps");
  }

  private void basePushAggThroughUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateUnionTransposeRule.INSTANCE)
        .build();
    checkPlanning(program, "${sql}");
  }

  @Test public void testPushSumConstantThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumNullConstantThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumNullableNOGBYThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushCountStarThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushCountNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushMaxNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushMinThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushAvgThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumCountStarThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumConstantGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumNullConstantGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushCountStarGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushCountNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushMaxNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushMinGroupingSetsThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushAvgGroupingSetsThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPushSumCountStarGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test public void testPullFilterThroughAggregate() throws Exception {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)
        .build();
    HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateFilterTransposeRule.INSTANCE)
        .build();
    checkPlanning(tester, preProgram,
        new HepPlanner(program),
        "select empno, sal, deptno from ("
            + "  select empno, sal, deptno"
            + "  from emp"
            + "  where sal > 5000)"
            + "group by empno, sal, deptno");
  }

  @Test public void testPullFilterThroughAggregateGroupingSets()
      throws Exception {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)
        .build();
    HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateFilterTransposeRule.INSTANCE)
        .build();
    checkPlanning(tester, preProgram,
        new HepPlanner(program),
        "select empno, sal, deptno from ("
            + "  select empno, sal, deptno"
            + "  from emp"
            + "  where sal > 5000)"
            + "group by rollup(empno, sal, deptno)");
  }

  private void basePullConstantTroughAggregate() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateProjectPullUpConstantsRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program, "${sql}");
  }

  @Test public void testPullConstantThroughConstLast() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregateSimpleNonNullable() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregatePermuted() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregatePermutedConstFirst() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregatePermutedConstGroupBy()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregateConstGroupBy()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregateAllConst()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testPullConstantThroughAggregateAllLiterals()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test public void testAggregateProjectMerge() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select x, sum(z), y from (\n"
            + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
            + "  from emp)\n"
            + "group by x, y");
  }

  @Test public void testAggregateGroupingSetsProjectMerge() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select x, sum(z), y from (\n"
            + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
            + "  from emp)\n"
            + "group by rollup(x, y)");
  }

  @Test public void testPullAggregateThroughUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateUnionAggregateRule.INSTANCE)
        .build();

    checkPlanning(program,
        "select deptno, job from"
            + " (select deptno, job from emp as e1"
            + " group by deptno,job"
            + "  union all"
            + " select deptno, job from emp as e2"
            + " group by deptno,job)"
            + " group by deptno,job");
  }

  public void transitiveInference(RelOptRule... extraRules) throws Exception {
    final DiffRepository diffRepos = getDiffRepos();
    String sql = diffRepos.expand(null, "${sql}");

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.DUMB_FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterSetOpTransposeRule.INSTANCE)
        .build();
    HepPlanner planner = new HepPlanner(program);

    RelNode relInitial = tester.convertSqlToRel(sql);

    assertTrue(relInitial != null);

    List<RelMetadataProvider> list = Lists.newArrayList();
    DefaultRelMetadataProvider defaultProvider =
        new DefaultRelMetadataProvider();
    list.add(defaultProvider);
    planner.registerMetadataProviders(list);
    RelMetadataProvider plannerChain = ChainedRelMetadataProvider.of(list);
    relInitial.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(plannerChain, planner));

    planner.setRoot(relInitial);
    RelNode relAfter = planner.findBestExp();

    String planBefore = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    HepProgram program2 = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(FilterJoinRule.DUMB_FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterSetOpTransposeRule.INSTANCE)
        .addRuleInstance(JoinPushTransitivePredicatesRule.INSTANCE)
        .addRuleCollection(Arrays.asList(extraRules))
        .build();
    HepPlanner planner2 = new HepPlanner(program2);
    planner.registerMetadataProviders(list);
    planner2.setRoot(relAfter);
    relAfter = planner2.findBestExp();

    String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testTransitiveInferenceJoin() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceProject() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceAggregate() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceUnion() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceJoin3way() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceJoin3wayAgg() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceLeftOuterJoin() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceRightOuterJoin() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceFullOuterJoin() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferencePreventProjectPullUp()
      throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferencePullUpThruAlias() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceConjunctInPullUp() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceNoPullUpExprs() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceUnion3way() throws Exception {
    transitiveInference();
  }

  @Ignore("not working")
  @Test public void testTransitiveInferenceUnion3wayOr() throws Exception {
    transitiveInference();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-443">[CALCITE-443],
   * getPredicates from a union is not correct</a>. */
  @Test public void testTransitiveInferenceUnionAlwaysTrue() throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceConstantEquiPredicate()
      throws Exception {
    transitiveInference();
  }

  @Test public void testTransitiveInferenceComplexPredicate() throws Exception {
    transitiveInference();
  }

  @Test public void testPullConstantIntoProject() throws Exception {
    transitiveInference(ReduceExpressionsRule.PROJECT_INSTANCE);
  }

  @Test public void testPullConstantIntoFilter() throws Exception {
    transitiveInference(ReduceExpressionsRule.FILTER_INSTANCE);
  }

  @Test public void testPullConstantIntoJoin() throws Exception {
    transitiveInference(ReduceExpressionsRule.JOIN_INSTANCE);
  }

  @Test public void testPullConstantIntoJoin2() throws Exception {
    transitiveInference(ReduceExpressionsRule.JOIN_INSTANCE,
        ReduceExpressionsRule.PROJECT_INSTANCE,
        FilterProjectTransposeRule.INSTANCE);
  }

  @Test public void testPushFilterWithRank() throws Exception {
    HepProgram program = new HepProgramBuilder().addRuleInstance(
        FilterProjectTransposeRule.INSTANCE).build();
    checkPlanning(program, "select e1.ename, r\n"
        + "from (\n"
        + "  select ename, "
        + "  rank() over(partition by  deptno order by sal) as r "
        + "  from emp) e1\n"
        + "where r < 2");
  }

  @Test public void testPushFilterWithRankExpr() throws Exception {
    HepProgram program = new HepProgramBuilder().addRuleInstance(
        FilterProjectTransposeRule.INSTANCE).build();
    checkPlanning(program, "select e1.ename, r\n"
        + "from (\n"
        + "  select ename,\n"
        + "  rank() over(partition by  deptno order by sal) + 1 as r "
        + "  from emp) e1\n"
        + "where r < 2");
  }

  @Test public void testPushAggregateThroughJoin1() throws Exception {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.INSTANCE)
        .build();
    checkPlanning(tester, preProgram,
        new HepPlanner(program),
        "select e.empno,d.deptno \n"
                + "from (select * from sales.emp where empno = 10) as e "
                + "join sales.dept as d on e.empno = d.deptno "
                + "group by e.empno,d.deptno");
  }

  @Test public void testPushAggregateThroughJoin2() throws Exception {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.INSTANCE)
        .build();
    checkPlanning(tester, preProgram,
        new HepPlanner(program),
        "select e.empno,d.deptno \n"
                + "from (select * from sales.emp where empno = 10) as e "
                + "join sales.dept as d on e.empno = d.deptno "
                + "and e.deptno + e.empno = d.deptno + 5 "
                + "group by e.empno,d.deptno");
  }

  @Test public void testPushAggregateThroughJoin3() throws Exception {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.INSTANCE)
        .build();
    checkPlanning(tester, preProgram,
        new HepPlanner(program),
        "select e.empno,d.deptno \n"
                + "from (select * from sales.emp where empno = 10) as e "
                + "join sales.dept as d on e.empno < d.deptno "
                + "group by e.empno,d.deptno");
  }

  @Test public void testPushAggregateThroughJoin4() throws Exception {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.INSTANCE)
        .build();
    checkPlanning(tester, preProgram,
        new HepPlanner(program),
        "select e.empno,sum(sal) \n"
                + "from (select * from sales.emp where empno = 10) as e "
                + "join sales.dept as d on e.empno = d.deptno "
                + "group by e.empno,d.deptno");
  }

  @Test public void testSwapOuterJoin() throws Exception {
    final HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(JoinCommuteRule.SWAP_OUTER)
        .build();
    checkPlanning(program,
        "select 1 from sales.dept d left outer join sales.emp e"
            + " on d.deptno = e.deptno");
  }
}

// End RelOptRulesTest.java
