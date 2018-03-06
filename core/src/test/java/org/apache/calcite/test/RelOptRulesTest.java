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

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rel.rules.AggregateFilterTransposeRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rel.rules.AggregateValuesRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.rules.JoinAddRedundantSemiJoinRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinExtractFilterRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.JoinUnionTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinJoinTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for rules in {@code org.apache.calcite.rel} and subpackages.
 *
 * <p>As input, the test supplies a SQL statement and a single rule; the SQL is
 * translated into relational algebra and then fed into a
 * {@link org.apache.calcite.plan.hep.HepPlanner}. The planner fires the rule on
 * every
 * pattern match in a depth-first left-to-right pre-order traversal of the tree
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

  @Test public void testReduceNot() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.FILTER_INSTANCE);

    final String sql = "select *\n"
        + "from (select (case when sal > 1000 then null else false end) as caseCol from emp)\n"
        + "where NOT(caseCol)";
    checkPlanning(tester, preProgram, hepPlanner, sql, true);
  }

  @Test public void testReduceNestedCaseWhen() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.FILTER_INSTANCE);

    final String sql = "select sal\n"
            + "from emp\n"
            + "where case when (sal = 1000) then\n"
            + "(case when sal = 1000 then null else 1 end is null) else\n"
            + "(case when sal = 2000 then null else 1 end is null) end is true";
    checkPlanning(tester, preProgram, hepPlanner, sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1479">[CALCITE-1479]
   * AssertionError in ReduceExpressionsRule on multi-column IN
   * sub-query</a>. */
  @Test public void testReduceCompositeInSubQuery() {
    final HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();
    final String sql = "select *\n"
        + "from emp\n"
        + "where (empno, deptno) in (\n"
        + "  select empno, deptno from (\n"
        + "    select empno, deptno\n"
        + "    from emp\n"
        + "    group by empno, deptno))\n"
        + "or deptno < 40 + 60";
    checkSubQuery(sql)
        .with(hepProgram)
        .check();
  }

  @Test public void testReduceOrCaseWhen() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.FILTER_INSTANCE);

    final String sql = "select sal\n"
        + "from emp\n"
        + "where case when sal = 1000 then null else 1 end is null\n"
        + "OR case when sal = 2000 then null else 1 end is null";
    checkPlanning(tester, preProgram, hepPlanner, sql);
  }

  @Test public void testReduceNullableCase() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);

    final String sql = "SELECT CASE WHEN 1=2 "
        + "THEN cast((values(1)) as integer) "
        + "ELSE 2 end from (values(1))";
    sql(sql).with(hepPlanner).check();
  }

  @Test public void testReduceNullableCase2() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);

    final String sql = "SELECT deptno, ename, CASE WHEN 1=2 "
        + "THEN substring(ename, 1, cast(2 as int)) ELSE NULL end from emp"
        + " group by deptno, ename, case when 1=2 then substring(ename,1, cast(2 as int))  else null end";
    sql(sql).with(hepPlanner).check();
  }

  @Test public void testProjectToWindowRuleForMultipleWindows() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectToWindowRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ProjectToWindowRule.PROJECT);

    final String sql = "select\n"
        + " count(*) over(partition by empno order by sal) as count1,\n"
        + " count(*) over(partition by deptno order by sal) as count2,\n"
        + " sum(deptno) over(partition by empno order by sal) as sum1,\n"
        + " sum(deptno) over(partition by deptno order by sal) as sum2\n"
        + "from emp";
    checkPlanning(tester, preProgram, hepPlanner, sql);
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
    final String sql = "select *\n"
        + "from dept left join emp using (deptno)\n"
        + "where emp.deptno is not null and emp.sal > 100";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .check();
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

  private void basePushFilterPastAggWithGroupingSets(boolean unchanged)
      throws Exception {
    final HepProgram preProgram =
            HepProgram.builder()
                .addRuleInstance(ProjectMergeRule.INSTANCE)
                .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
                .build();
    final HepProgram program =
            HepProgram.builder()
                .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
                .build();
    checkPlanning(tester, preProgram, new HepPlanner(program), "${sql}",
        unchanged);
  }

  @Test public void testPushFilterPastAggWithGroupingSets1() throws Exception {
    basePushFilterPastAggWithGroupingSets(true);
  }

  @Test public void testPushFilterPastAggWithGroupingSets2() throws Exception {
    basePushFilterPastAggWithGroupingSets(false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-434">[CALCITE-434]
   * FilterAggregateTransposeRule loses conditions that cannot be pushed</a>. */
  @Test public void testPushFilterPastAggTwo() {
    checkPlanning(FilterAggregateTransposeRule.INSTANCE,
        "select dept1.c1 from (\n"
            + "  select dept.name as c1, count(*) as c2\n"
            + "  from dept where dept.name > 'b' group by dept.name) dept1\n"
            + "where dept1.c1 > 'c' and (dept1.c2 > 30 or dept1.c1 < 'z')");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-799">[CALCITE-799]
   * Incorrect result for {@code HAVING count(*) > 1}</a>. */
  @Test public void testPushFilterPastAggThree() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
            .build();
    final String sql = "select deptno from emp\n"
        + "group by deptno having count(*) > 1";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1109">[CALCITE-1109]
   * FilterAggregateTransposeRule pushes down incorrect condition</a>. */
  @Test public void testPushFilterPastAggFour() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
            .addRuleInstance(AggregateFilterTransposeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
            .build();
    checkPlanning(tester, preProgram, new HepPlanner(program),
        "select emp.deptno, count(*) from emp where emp.sal > '12' "
            + "group by emp.deptno\n", false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-448">[CALCITE-448]
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
        new FilterJoinRule.JoinConditionPushRule(RelBuilder.proto(), predicate);
    final FilterJoinRule filterOnJoin =
        new FilterJoinRule.FilterIntoJoinRule(true, RelBuilder.proto(),
            predicate);
    final HepProgram program =
        HepProgram.builder()
            .addGroupBegin()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(join)
            .addRuleInstance(filterOnJoin)
            .addGroupEnd()
            .build();
    final String sql = "select a.name\n"
        + "from dept a\n"
        + "left join dept b on b.deptno > 10\n"
        + "right join dept c on b.deptno > 10\n";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testJoinProjectTranspose() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .addRuleInstance(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER)
            .addRuleInstance(JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final String sql = "select a.name\n"
        + "from dept a\n"
        + "left join dept b on b.deptno > 10\n"
        + "right join dept c on b.deptno > 10\n";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-889">[CALCITE-889]
   * Implement SortUnionTransposeRule</a>. */
  @Test public void testSortUnionTranspose() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(SortUnionTransposeRule.INSTANCE)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 10";
    checkPlanning(program, sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-889">[CALCITE-889]
   * Implement SortUnionTransposeRule</a>. */
  @Test public void testSortUnionTranspose2() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(SortUnionTransposeRule.MATCH_NULL_FETCH)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name";
    checkPlanning(program, sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-987">[CALCITE-987]
   * Push limit 0 will result in an infinite loop</a>. */
  @Test public void testSortUnionTranspose3() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(SortUnionTransposeRule.MATCH_NULL_FETCH)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 0";
    checkPlanning(program, sql);
  }

  @Test public void testSemiJoinRuleExists() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(SemiJoinRule.PROJECT)
            .build();
    final String sql = "select * from dept where exists (\n"
        + "  select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.sal > 100)";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .check();
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
            .addRuleInstance(SemiJoinRule.PROJECT)
            .build();
    final String sql = "select dept.* from dept join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1495">[CALCITE-1495]
   * SemiJoinRule should not apply to RIGHT and FULL JOIN</a>. */
  @Test public void testSemiJoinRuleRight() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(SemiJoinRule.PROJECT)
            .build();
    final String sql = "select dept.* from dept right join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .withDecorrelation(true)
        .withTrim(true)
        .checkUnchanged();
  }

  /** Similar to {@link #testSemiJoinRuleRight()} but FULL. */
  @Test public void testSemiJoinRuleFull() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(SemiJoinRule.PROJECT)
            .build();
    final String sql = "select dept.* from dept full join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .withDecorrelation(true)
        .withTrim(true)
        .checkUnchanged();
  }

  /** Similar to {@link #testSemiJoinRule()} but LEFT. */
  @Test public void testSemiJoinRuleLeft() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(SemiJoinRule.PROJECT)
            .build();
    final String sql = "select name from dept left join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .withDecorrelation(true)
        .withTrim(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-438">[CALCITE-438]
   * Push predicates through SemiJoin</a>. */
  @Test public void testPushFilterThroughSemiJoin() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(SemiJoinRule.PROJECT)
            .build();

    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(FilterJoinRule.JOIN)
            .build();
    final String sql = "select * from (\n"
        + "  select * from dept where dept.deptno in (\n"
        + "    select emp.deptno from emp))R\n"
        + "where R.deptno <=10";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(false)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-571">[CALCITE-571]
   * ReduceExpressionsRule tries to reduce SemiJoin condition to non-equi
   * condition</a>. */
  @Test public void testSemiJoinReduceConstants() {
    final HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(SemiJoinRule.PROJECT)
        .build();
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();
    final String sql = "select e1.sal\n"
        + "from (select * from emp where deptno = 200) as e1\n"
        + "where e1.deptno in (\n"
        + "  select e2.deptno from emp e2 where e2.sal = 100)";
    sql(sql)
        .withDecorrelation(false)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .checkUnchanged();
  }

  @Test public void testSemiJoinTrim() throws Exception {
    final DiffRepository diffRepos = getDiffRepos();
    String sql = diffRepos.expand(null, "${sql}");

    TesterImpl t = (TesterImpl) tester;
    final RelDataTypeFactory typeFactory = t.getTypeFactory();
    final Prepare.CatalogReader catalogReader =
        t.createCatalogReader(typeFactory);
    final SqlValidator validator =
        t.createValidator(
            catalogReader, typeFactory);
    SqlToRelConverter converter =
        t.createSqlToRelConverter(
            validator,
            catalogReader,
            typeFactory,
            SqlToRelConverter.Config.DEFAULT);

    final SqlNode sqlQuery = t.parseQuery(sql);
    final SqlNode validatedQuery = validator.validate(sqlQuery);
    RelRoot root =
        converter.convertQuery(validatedQuery, false, true);
    root = root.withRel(converter.decorrelate(sqlQuery, root.rel));

    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .addRuleInstance(SemiJoinRule.PROJECT)
            .build();

    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root.rel);
    root = root.withRel(planner.findBestExp());

    String planBefore = NL + RelOptUtil.toString(root.rel);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    converter = t.createSqlToRelConverter(validator, catalogReader, typeFactory,
        SqlToRelConverter.configBuilder().withTrimUnusedFields(true).build());
    root = root.withRel(converter.trimUnusedFields(false, root.rel));
    String planAfter = NL + RelOptUtil.toString(root.rel);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test public void testReduceAverage() {
    checkPlanning(AggregateReduceFunctionsRule.INSTANCE,
        "select name, max(name), avg(deptno), min(name)"
            + " from sales.dept group by name");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1621">[CALCITE-1621]
   * Adding a cast around the null literal in aggregate rules</a>. */
  @Test public void testCastInAggregateReduceFunctions() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(AggregateReduceFunctionsRule.INSTANCE)
            .build();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " stddev_samp(deptno),var_pop(deptno), var_samp(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).with(program).check();
  }

  @Test public void testDistinctCountWithoutGroupBy() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select max(deptno), count(distinct ename)\n"
        + "from sales.emp";
    sql(sql).with(program).check();
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1293">[CALCITE-1293]
   * Bad code generated when argument to COUNT(DISTINCT) is a # GROUP BY
   * column</a>. */
  @Test public void testDistinctCount3() {
    final String sql = "select count(distinct deptno), sum(sal)"
        + " from sales.emp group by deptno";
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .build();
    sql(sql).with(program).check();
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

  @Test public void testDistinctNonDistinctAggregates() {
    final String sql = "select emp.empno, count(*), avg(distinct dept.deptno)\n"
        + "from sales.emp emp inner join sales.dept dept\n"
        + "on emp.deptno = dept.deptno\n"
        + "group by emp.empno";
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1621">[CALCITE-1621]
   * Adding a cast around the null literal in aggregate rules</a>. */
  @Test public void testCastInAggregateExpandDistinctAggregatesRule() {
    final String sql = "select name, sum(distinct cn), sum(distinct sm)\n"
        + "from (\n"
        + "  select name, count(dept.deptno) as cn,sum(dept.deptno) as sm\n"
        + "  from sales.dept group by name)\n"
        + "group by name";
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .build();
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1558">[CALCITE-1558]
   * AggregateExpandDistinctAggregatesRule gets field mapping wrong if groupKey
   * is used in aggregate function</a>. */
  @Test public void testDistinctNonDistinctAggregatesWithGrouping1() {
    final String sql = "SELECT deptno,\n"
        + "  SUM(deptno), SUM(DISTINCT sal), MAX(deptno), MAX(comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test public void testDistinctNonDistinctAggregatesWithGrouping2() {
    final String sql = "SELECT deptno, COUNT(deptno), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test public void testDistinctNonDistinctTwoAggregatesWithGrouping() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test public void testDistinctWithGrouping() {
    final String sql = "SELECT sal, SUM(comm), MIN(comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY sal";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }


  @Test public void testMultipleDistinctWithGrouping() {
    final String sql = "SELECT sal, SUM(comm), MIN(DISTINCT comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY sal";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test public void testDistinctWithMultipleInputs() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), COUNT(DISTINCT sal, comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test public void testDistinctWithMultipleInputsAndGroupby() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), COUNT(DISTINCT sal, deptno, comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test public void testPushProjectPastFilter() {
    checkPlanning(ProjectFilterTransposeRule.INSTANCE,
        "select empno + deptno from emp where sal = 10 * comm "
            + "and upper(ename) = 'FOO'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1778">[CALCITE-1778]
   * Query with "WHERE CASE" throws AssertionError "Cast for just nullability
   * not allowed"</a>. */
  @Test public void testPushProjectPastFilter2() {
    final String sql = "select count(*)\n"
        + "from emp\n"
        + "where case when mgr < 10 then true else false end";
    sql(sql).withRule(ProjectFilterTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastJoin() {
    checkPlanning(ProjectJoinTransposeRule.INSTANCE,
        "select e.sal + b.comm from emp e inner join bonus b "
            + "on e.ename = b.ename and e.deptno = 10");
  }

  private static final String NOT_STRONG_EXPR =
      "case when e.sal < 11 then 11 else -1 * e.sal end";

  private static final String STRONG_EXPR =
      "case when e.sal < 11 then -1 * e.sal else e.sal end";

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1753">[CALCITE-1753]
   * PushProjector should only preserve expressions if the expression is strong
   * when pushing into the nullable-side of outer join</a>. */
  @Test public void testPushProjectPastInnerJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e inner join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastInnerJoinStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from emp e inner join bonus b on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastLeftJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e left outer join bonus b on e.ename = b.ename\n"
        + "group by case when e.sal < 11 then 11 else -1 * e.sal end";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastLeftJoinSwap() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from bonus b left outer join emp e on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastLeftJoinSwapStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from bonus b left outer join emp e on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastRightJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e right outer join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastRightJoinStrong() {
    final String sql = "select count(*),\n"
        + " case when e.sal < 11 then -1 * e.sal else e.sal end\n"
        + "from emp e right outer join bonus b on e.ename = b.ename\n"
        + "group by case when e.sal < 11 then -1 * e.sal else e.sal end";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastRightJoinSwap() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from bonus b right outer join emp e on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastRightJoinSwapStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from bonus b right outer join emp e on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastFullJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e full outer join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test public void testPushProjectPastFullJoinStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from emp e full outer join bonus b on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
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

  /** Tests to see if the final branch of union is missed */
  @Test
  public void testUnionMergeRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(ProjectRemoveRule.INSTANCE)
            .addRuleInstance(UnionMergeRule.INSTANCE)
            .build();

    checkPlanning(program,
            "select * from (\n"
                    + "select * from (\n"
                    + "  select name, deptno from dept\n"
                    + "  union all\n"
                    + "  select name, deptno from\n"
                    + "  (\n"
                    + "    select name, deptno, count(1) from dept group by name, deptno\n"
                    + "    union all\n"
                    + "    select name, deptno, count(1) from dept group by name, deptno\n"
                    + "  ) subq\n"
                    + ") a\n"
                    + "union all\n"
                    + "select name, deptno from dept\n"
                    + ") aa\n");
  }

  @Test
  public void testMinusMergeRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(ProjectRemoveRule.INSTANCE)
            .addRuleInstance(UnionMergeRule.MINUS_INSTANCE)
            .build();

    checkPlanning(program,
            "select * from (\n"
                    + "select * from (\n"
                    + "  select name, deptno from\n"
                    + "  (\n"
                    + "    select name, deptno, count(1) from dept group by name, deptno\n"
                    + "    except all\n"
                    + "    select name, deptno, 1 from dept\n"
                    + "  ) subq\n"
                    + "  except all\n"
                    + "  select name, deptno from\n"
                    + "  (\n"
                    + "    select name, deptno, 1 from dept\n"
                    + "    except all\n"
                    + "    select name, deptno, count(1) from dept group by name, deptno\n"
                    + "  ) subq2\n"
                    + ") a\n"
                    + "except all\n"
                    + "select name, deptno from dept\n"
                    + ") aa\n");
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

  /** Tests {@link UnionMergeRule}, which merges 2 {@link Union} operators into
   * a single {@code Union} with 3 inputs. */
  @Test public void testMergeUnionAll() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union all\n"
        + "select * from emp where deptno = 20\n"
        + "union all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link UnionMergeRule}, which merges 2 {@link Union}
   * {@code DISTINCT} (not {@code ALL}) operators into a single
   * {@code Union} with 3 inputs. */
  @Test public void testMergeUnionDistinct() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union distinct\n"
        + "select * from emp where deptno = 20\n"
        + "union\n" // same as 'union distinct'
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests that {@link UnionMergeRule} does nothing if its arguments have
   * different {@code ALL} settings. */
  @Test public void testMergeUnionMixed() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union\n"
        + "select * from emp where deptno = 20\n"
        + "union all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).checkUnchanged();
  }

  /** Tests that {@link UnionMergeRule} converts all inputs to DISTINCT
   * if the top one is DISTINCT.
   * (Since UNION is left-associative, the "top one" is the rightmost.) */
  @Test public void testMergeUnionMixed2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union all\n"
        + "select * from emp where deptno = 20\n"
        + "union\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests that {@link UnionMergeRule} does nothing if its arguments have
   * are different set operators, {@link Union} and {@link Intersect}. */
  @Test public void testMergeSetOpMixed() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.INSTANCE)
        .addRuleInstance(UnionMergeRule.INTERSECT_INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "union\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).checkUnchanged();
  }

  /** Tests {@link UnionMergeRule#INTERSECT_INSTANCE}, which merges 2
   * {@link Intersect} operators into a single {@code Intersect} with 3
   * inputs. */
  @Test public void testMergeIntersect() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.INTERSECT_INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link org.apache.calcite.rel.rules.IntersectToDistinctRule},
   * which rewrites an {@link Intersect} operator with 3 inputs. */
  @Test public void testIntersectToDistinct() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.INTERSECT_INSTANCE)
        .addRuleInstance(IntersectToDistinctRule.INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests that {@link org.apache.calcite.rel.rules.IntersectToDistinctRule}
   * correctly ignores an {@code INTERSECT ALL}. It can only handle
   * {@code INTERSECT DISTINCT}. */
  @Test public void testIntersectToDistinctAll() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.INTERSECT_INSTANCE)
        .addRuleInstance(IntersectToDistinctRule.INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link UnionMergeRule#MINUS_INSTANCE}, which merges 2
   * {@link Minus} operators into a single {@code Minus} with 3
   * inputs. */
  @Test public void testMergeMinus() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.MINUS_INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "except\n"
        + "select * from emp where deptno = 20\n"
        + "except\n"
        + "select * from emp where deptno = 30\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link UnionMergeRule#MINUS_INSTANCE}
   * does not merge {@code Minus(a, Minus(b, c))}
   * into {@code Minus(a, b, c)}, which would be incorrect. */
  @Test public void testMergeMinusRightDeep() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(UnionMergeRule.MINUS_INSTANCE)
        .build();

    final String sql = "select * from emp where deptno = 10\n"
        + "except\n"
        + "select * from (\n"
        + "  select * from emp where deptno = 20\n"
        + "  except\n"
        + "  select * from emp where deptno = 30)";
    sql(sql).with(program).checkUnchanged();
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
    final String sql = "select"
        + " 1+2, d.deptno+(3+4), (5+6)+d.deptno, cast(null as integer),"
        + " coalesce(2,null), row(7+8)"
        + " from dept d inner join emp e"
        + " on d.deptno = e.deptno + (5-5)"
        + " where d.deptno=(7+8) and d.deptno=(8+7) and d.deptno=coalesce(2,null)";
    sql(sql).with(program)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-570">[CALCITE-570]
   * ReduceExpressionsRule throws "duplicate key" exception</a>. */
  @Test public void testReduceConstantsDup() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    final String sql = "select d.deptno"
        + " from dept d"
        + " where d.deptno=7 and d.deptno=8";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-935">[CALCITE-935]
   * Improve how ReduceExpressionsRule handles duplicate constraints</a>. */
  @Test public void testReduceConstantsDup2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    final String sql = "select *\n"
        + "from emp\n"
        + "where deptno=7 and deptno=8\n"
        + "and empno = 10 and mgr is null and empno = 10";
    checkPlanning(program, sql);
  }

  @Test public void testPullNull() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    final String sql = "select *\n"
        + "from emp\n"
        + "where deptno=7\n"
        + "and empno = 10 and mgr is null and empno = 10";
    checkPlanning(program, sql);
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-902">[CALCITE-902]
   * Match nullability when reducing expressions in a Project</a>. */
  @Test public void testReduceConstantsProjectNullable() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    checkPlanning(program, "select mgr from emp where mgr=10");
  }

  // see HIVE-9645
  @Test public void testReduceConstantsNullEqualsOne() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    checkPlanning(program,
        "select count(1) from emp where cast(null as integer) = 1");
  }

  // see HIVE-9644
  @Test public void testReduceConstantsCaseEquals() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    // Equivalent to 'deptno = 10'
    checkPlanning(program,
        "select count(1) from emp\n"
            + "where case deptno\n"
            + "  when 20 then 2\n"
            + "  when 10 then 1\n"
            + "  else 3 end = 1");
  }

  @Test public void testReduceConstantsCaseEquals2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    // Equivalent to 'case when deptno = 20 then false
    //                     when deptno = 10 then true
    //                     else null end'
    checkPlanning(program,
        "select count(1) from emp\n"
            + "where case deptno\n"
            + "  when 20 then 2\n"
            + "  when 10 then 1\n"
            + "  else cast(null as integer) end = 1");
  }

  @Test public void testReduceConstantsCaseEquals3() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    // Equivalent to 'deptno = 30 or deptno = 10'
    checkPlanning(program,
        "select count(1) from emp\n"
            + "where case deptno\n"
            + "  when 30 then 1\n"
            + "  when 20 then 2\n"
            + "  when 10 then 1\n"
            + "  when 30 then 111\n"
            + "  else 0 end = 1");
  }

  @Test
  public void testSkipReduceConstantsCaseEquals() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN)
        .build();

    checkPlanning(program,
        "select * from emp e1, emp e2\n"
            + "where coalesce(e1.mgr, -1) = coalesce(e2.mgr, -1)");
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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1860">[CALCITE-1860]
   * Duplicate null predicates cause NullPointerException in RexUtil</a>. */
  @Test public void testReduceConstantsNull() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();
    final String sql = "select * from (\n"
        + "  select *\n"
        + "  from (\n"
        + "    select cast(null as integer) as n\n"
        + "    from emp)\n"
        + "  where n is null and n is null)\n"
        + "where n is null";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-566">[CALCITE-566]
   * ReduceExpressionsRule requires planner to have an Executor</a>. */
  @Test public void testReduceConstantsRequiresExecutor() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    // Remove the executor
    tester.convertSqlToRel("values 1").rel.getCluster().getPlanner()
        .setExecutor(null);

    // Rule should not fire, but there should be no NPE
    final String sql =
        "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)";
    checkPlanUnchanged(new HepPlanner(program), sql);
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
                  MockTable table = MockTable.create(this, schema, t, false, 100);
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1439">[CALCITE-1439]
   * Handling errors during constant reduction</a>. */
  @Test public void testReduceCase() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .build();

    final String sql = "select\n"
        + "  case when false then cast(2.1 as float)\n"
        + "   else cast(1 as integer) end as newcol\n"
        + "from emp";
    sql(sql).with(program)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  private void checkReduceNullableToNotNull(ReduceExpressionsRule rule) {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(rule)
        .build();

    final String sql = "select\n"
        + "  empno + case when 'a' = 'a' then 1 else null end as newcol\n"
        + "from emp";
    sql(sql).with(program)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  /** Test case that reduces a nullable expression to a NOT NULL literal that
   *  is cast to nullable. */
  @Test public void testReduceNullableToNotNull() throws Exception {
    checkReduceNullableToNotNull(ReduceExpressionsRule.PROJECT_INSTANCE);
  }

  /** Test case that reduces a nullable expression to a NOT NULL literal. */
  @Test public void testReduceNullableToNotNull2() throws Exception {
    final ReduceExpressionsRule.ProjectReduceExpressionsRule rule =
        new ReduceExpressionsRule.ProjectReduceExpressionsRule(
            LogicalProject.class, false,
            RelFactories.LOGICAL_BUILDER);
    checkReduceNullableToNotNull(rule);
  }

  @Test public void testReduceConstantsIsNull() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    checkPlanning(program,
        "select empno from emp where empno=10 and empno is null");
  }

  @Test public void testReduceConstantsIsNotNull() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    final String sql = "select empno from emp\n"
        + "where empno=10 and empno is not null";
    checkPlanning(program, sql);
  }

  @Test public void testReduceConstantsNegated() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    final String sql = "select empno from emp\n"
        + "where empno=10 and not(empno=10)";
    checkPlanning(program, sql);
  }

  @Test public void testReduceConstantsNegatedInverted() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    final String sql = "select empno from emp where empno>10 and empno<=10";
    checkPlanning(program, sql);
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1488">[CALCITE-1488]
   * ValuesReduceRule should ignore empty Values</a>. */
  @Test public void testEmptyProject() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .addRuleInstance(ValuesReduceRule.FILTER_INSTANCE)
        .addRuleInstance(ValuesReduceRule.PROJECT_INSTANCE)
        .build();

    final String sql = "select z + x from (\n"
        + "  select x + y as z, x from (\n"
        + "    select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "    where x + y > 50))";
    sql(sql).with(program).check();
  }

  /** Same query as {@link #testEmptyProject()}, and {@link PruneEmptyRules}
   * is able to do the job that {@link ValuesReduceRule} cannot do. */
  @Test public void testEmptyProject2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ValuesReduceRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();

    final String sql = "select z + x from (\n"
        + "  select x + y as z, x from (\n"
        + "    select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "    where x + y > 50))";
    sql(sql).with(program).check();
  }

  @Test public void testEmptyIntersect() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.INTERSECT_INSTANCE)
        .build();

    final String sql = "select * from (values (30, 3))"
        + "intersect\n"
        + "select *\nfrom (values (10, 1), (30, 3)) as t (x, y) where x > 50\n"
        + "intersect\n"
        + "select * from (values (30, 3))";
    sql(sql).with(program).check();
  }

  @Test public void testEmptyMinus() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.MINUS_INSTANCE)
        .build();

    // First input is empty; therefore whole expression is empty
    final String sql = "select * from (values (30, 3)) as t (x, y)\n"
        + "where x > 30\n"
        + "except\n"
        + "select * from (values (20, 2))\n"
        + "except\n"
        + "select * from (values (40, 4))";
    sql(sql).with(program).check();
  }

  @Test public void testEmptyMinus2() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.MINUS_INSTANCE)
        .build();

    // Second and fourth inputs are empty; they are removed
    final String sql = "select * from (values (30, 3)) as t (x, y)\n"
        + "except\n"
        + "select * from (values (20, 2)) as t (x, y) where x > 30\n"
        + "except\n"
        + "select * from (values (40, 4))\n"
        + "except\n"
        + "select * from (values (50, 5)) as t (x, y) where x > 50";
    sql(sql).with(program).check();
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

  @Test public void testEmptyAggregate() {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.AGGREGATE_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();

    final String sql = "select sum(empno) from emp where false group by deptno";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testEmptyAggregateEmptyKey() {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PruneEmptyRules.AGGREGATE_INSTANCE)
        .build();

    final String sql = "select sum(empno) from emp where false";
    final boolean unchanged = true;
    checkPlanning(tester, preProgram, new HepPlanner(program), sql, unchanged);
  }

  @Test public void testEmptyAggregateEmptyKeyWithAggregateValuesRule() {
    HepProgram preProgram = HepProgram
        .builder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateValuesRule.INSTANCE)
        .build();

    final String sql = "select count(*), sum(empno) from emp where false";
    sql(sql).withPre(preProgram).with(program).check();
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

  /** Tests that a cast from a TIME to a TIMESTAMP is not reduced. It is not
   * constant because the result depends upon the current date. */
  @Test public void testReduceCastTimeUnchanged() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    sql("select cast(time '12:34:56' as timestamp) from emp as e")
        .with(program)
        .checkUnchanged();
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
            new CoerceInputsRule(LogicalTableModify.class, false,
                RelFactories.LOGICAL_BUILDER))

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

  @Test public void testPushCountFilterThroughUnion() throws Exception {
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
    final String sql = "select ename, sal, deptno from ("
        + "  select ename, sal, deptno"
        + "  from emp"
        + "  where sal > 5000)"
        + "group by ename, sal, deptno";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
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
    final String sql = "select ename, sal, deptno from ("
        + "  select ename, sal, deptno"
        + "  from emp"
        + "  where sal > 5000)"
        + "group by rollup(ename, sal, deptno)";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
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

  @Test public void testPullConstantThroughUnion()
      throws Exception {
    HepProgram program = HepProgram.builder()
        .addRuleInstance(UnionPullUpConstantsRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select 2, deptno, job from emp as e1\n"
        + "union all\n"
        + "select 2, deptno, job from emp as e2";
    sql(sql)
        .withTrim(true)
        .with(program)
        .check();
  }

  @Test public void testPullConstantThroughUnion2()
      throws Exception {
    // Negative test: constants should not be pulled up
    HepProgram program = HepProgram.builder()
        .addRuleInstance(UnionPullUpConstantsRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select 2, deptno, job from emp as e1\n"
        + "union all\n"
        + "select 1, deptno, job from emp as e2";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  @Test public void testPullConstantThroughUnion3()
      throws Exception {
    // We should leave at least a single column in each Union input
    HepProgram program = HepProgram.builder()
        .addRuleInstance(UnionPullUpConstantsRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select 2, 3 from emp as e1\n"
        + "union all\n"
        + "select 2, 3 from emp as e2";
    sql(sql)
        .withTrim(true)
        .with(program)
        .check();
  }

  @Test public void testAggregateProjectMerge() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select x, sum(z), y from (\n"
            + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
            + "  from emp)\n"
            + "group by x, y");
  }

  @Test public void testAggregateGroupingSetsProjectMerge() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select x, sum(z), y from (\n"
            + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
            + "  from emp)\n"
            + "group by rollup(x, y)");
  }

  @Test public void testAggregateExtractProjectRule() {
    final String sql = "select sum(sal)\n"
        + "from emp";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final AggregateExtractProjectRule rule =
        new AggregateExtractProjectRule(Aggregate.class, LogicalTableScan.class,
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).check();
  }

  @Test public void testAggregateExtractProjectRuleWithGroupingSets() {
    final String sql = "select empno, deptno, sum(sal)\n"
        + "from emp\n"
        + "group by grouping sets ((empno, deptno),(deptno),(empno))";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final AggregateExtractProjectRule rule =
        new AggregateExtractProjectRule(Aggregate.class, LogicalTableScan.class,
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).check();
  }


  /** Test with column used in both grouping set and argument to aggregate
   * function. */
  @Test public void testAggregateExtractProjectRuleWithGroupingSets2() {
    final String sql = "select empno, deptno, sum(empno)\n"
        + "from emp\n"
        + "group by grouping sets ((empno, deptno),(deptno),(empno))";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final AggregateExtractProjectRule rule =
        new AggregateExtractProjectRule(Aggregate.class, LogicalTableScan.class,
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).check();
  }

  @Test public void testAggregateExtractProjectRuleWithFilter() {
    final String sql = "select sum(sal) filter (where empno = 40)\n"
        + "from emp";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    // AggregateProjectMergeRule does not merges Project with Filter.
    // Force match Aggregate on top of Project once explicitly in unit test.
    final AggregateExtractProjectRule rule =
        new AggregateExtractProjectRule(
            operand(Aggregate.class,
                operand(Project.class, null,
                    new PredicateImpl<Project>() {
                      int matchCount = 0;

                      public boolean test(@Nullable Project project) {
                        return matchCount++ == 0;
                      }
                    },
                    none())),
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).checkUnchanged();
  }

  @Test public void testPullAggregateThroughUnion() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateUnionAggregateRule.INSTANCE)
        .build();

    final String sql = "select deptno, job from"
        + " (select deptno, job from emp as e1"
        + " group by deptno,job"
        + "  union all"
        + " select deptno, job from emp as e2"
        + " group by deptno,job)"
        + " group by deptno,job";
    sql(sql).with(program).check();
  }

  @Test public void testPullAggregateThroughUnion2() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateUnionAggregateRule.AGG_ON_SECOND_INPUT)
        .addRuleInstance(AggregateUnionAggregateRule.AGG_ON_FIRST_INPUT)
        .build();

    final String sql = "select deptno, job from"
        + " (select deptno, job from emp as e1"
        + " group by deptno,job"
        + "  union all"
        + " select deptno, job from emp as e2"
        + " group by deptno,job)"
        + " group by deptno,job";
    sql(sql).with(program).check();
  }

  private void transitiveInference(RelOptRule... extraRules) throws Exception {
    final DiffRepository diffRepos = getDiffRepos();
    final String sql = diffRepos.expand(null, "${sql}");

    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.DUMB_FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterSetOpTransposeRule.INSTANCE)
        .build();
    final HepPlanner planner = new HepPlanner(program);

    final RelRoot root = tester.convertSqlToRel(sql);
    final RelNode relInitial = root.rel;

    assertTrue(relInitial != null);

    List<RelMetadataProvider> list = Lists.newArrayList();
    list.add(DefaultRelMetadataProvider.INSTANCE);
    planner.registerMetadataProviders(list);
    RelMetadataProvider plannerChain = ChainedRelMetadataProvider.of(list);
    relInitial.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(plannerChain, planner));

    planner.setRoot(relInitial);
    RelNode relBefore = planner.findBestExp();

    String planBefore = NL + RelOptUtil.toString(relBefore);
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
    final HepPlanner planner2 = new HepPlanner(program2);
    planner.registerMetadataProviders(list);
    planner2.setRoot(relBefore);
    RelNode relAfter = planner2.findBestExp();

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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-443">[CALCITE-443]
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1995">[CALCITE-1995]
   * Remove predicates from Filter if they can be proved to be always true or
   * false</a>. */
  @Test public void testSimplifyFilter() throws Exception {
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2110">[CALCITE-2110]
   * ArrayIndexOutOfBoundsException in RexSimplify when using
   * ReduceExpressionsRule.JOIN_INSTANCE</a>. */
  @Test public void testCorrelationScalarAggAndFilter() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();
    sql(sql)
        .withDecorrelation(true)
        .withTrim(true)
        .expand(true)
        .withPre(program)
        .with(program)
        .checkUnchanged();
  }

  @Test public void testProjectWindowTransposeRule() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectToWindowRule.PROJECT)
        .addRuleInstance(ProjectWindowTransposeRule.INSTANCE)
        .build();

    final String sql = "select count(empno) over(), deptno from emp";
    checkPlanning(program, sql);
  }

  @Test public void testProjectWindowTransposeRuleWithConstants() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectToWindowRule.PROJECT)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectWindowTransposeRule.INSTANCE)
        .build();

    final String sql = "select col1, col2\n"
        + "from (\n"
        + "  select empno,\n"
        + "    sum(100) over (partition by  deptno order by sal) as col1,\n"
        + "  sum(1000) over(partition by deptno order by sal) as col2\n"
        + "  from emp)";

    checkPlanning(program, sql);
  }

  @Test public void testAggregateProjectPullUpConstants() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .build();
    final String sql = "select job, empno, sal, sum(sal) as s\n"
        + "from emp where empno = 10\n"
        + "group by job, empno, sal";
    checkPlanning(program, sql);
  }

  @Test public void testPushFilterWithRank() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE).build();
    final String sql = "select e1.ename, r\n"
        + "from (\n"
        + "  select ename, "
        + "  rank() over(partition by  deptno order by sal) as r "
        + "  from emp) e1\n"
        + "where r < 2";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  @Test public void testPushFilterWithRankExpr() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE).build();
    final String sql = "select e1.ename, r\n"
        + "from (\n"
        + "  select ename,\n"
        + "  rank() over(partition by  deptno order by sal) + 1 as r "
        + "  from emp) e1\n"
        + "where r < 2";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-841">[CALCITE-841]
   * Redundant windows when window function arguments are expressions</a>. */
  @Test public void testExpressionInWindowFunction() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectToWindowRule.class);

    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ProjectToWindowRule.PROJECT);

    final String sql = "select\n"
        + " sum(deptno) over(partition by deptno order by sal) as sum1,\n"
        + "sum(deptno + sal) over(partition by deptno order by sal) as sum2\n"
        + "from emp";
    sql(sql)
        .with(hepPlanner)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-888">[CALCITE-888]
   * Overlay window loses PARTITION BY list</a>. */
  @Test public void testWindowInParenthesis() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectToWindowRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ProjectToWindowRule.PROJECT);

    final String sql = "select count(*) over (w), count(*) over w\n"
        + "from emp\n"
        + "window w as (partition by empno order by empno)";
    sql(sql)
        .with(hepPlanner)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-750">[CALCITE-750]
   * Allow windowed aggregate on top of regular aggregate</a>. */
  @Test public void testNestedAggregates() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(ProjectToWindowRule.PROJECT)
        .build();
    final String sql = "SELECT\n"
        + "  avg(sum(sal) + 2 * min(empno) + 3 * avg(empno))\n"
        + "  over (partition by deptno)\n"
        + "from emp\n"
        + "group by deptno";
    checkPlanning(program, sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2078">[CALCITE-2078]
   * Aggregate functions in OVER clause</a>. */
  @Test public void testWindowFunctionOnAggregations() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(ProjectToWindowRule.PROJECT)
        .build();
    final String sql = "SELECT\n"
        + "  min(empno),\n"
        + "  sum(sal),\n"
        + "  sum(sum(sal))\n"
        + "    over (partition by min(empno) order by sum(sal))\n"
        + "from emp\n"
        + "group by deptno";
    checkPlanning(program, sql);
  }

  @Test public void testPushAggregateThroughJoin1() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select e.job,d.name\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testPushAggregateThroughJoin2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select e.job,d.name\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job,d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testPushAggregateThroughJoin3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select e.empno,d.deptno\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.empno < d.deptno\n"
        + "group by e.empno,d.deptno";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql, true);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1544">[CALCITE-1544]
   * AggregateJoinTransposeRule fails to preserve row type</a>. */
  @Test public void testPushAggregateThroughJoin4() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select e.deptno\n"
        + "from sales.emp as e join sales.dept as d on e.deptno = d.deptno\n"
        + "group by e.deptno";
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test public void testPushAggregateThroughJoin5() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select e.deptno, d.deptno\n"
        + "from sales.emp as e join sales.dept as d on e.deptno = d.deptno\n"
        + "group by e.deptno, d.deptno";
    sql(sql).withPre(preProgram).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2200">[CALCITE-2200]
   * Infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test public void testJoinPushTransitivePredicatesRule() {
    HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(JoinPushTransitivePredicatesRule.INSTANCE)
        .build();

    final HepPlanner hepPlanner =
        new HepPlanner(new HepProgramBuilder().build());

    final String sql = "select d.deptno from sales.emp d where d.deptno\n"
        + "IN (select e.deptno from sales.emp e "
        + "where e.deptno = d.deptno or e.deptno = 4)";
    sql(sql).withPre(preProgram).with(hepPlanner).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2205">[CALCITE-2205]
   * One more infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test public void testJoinPushTransitivePredicatesRule2() {
    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(JoinPushTransitivePredicatesRule.INSTANCE)
        .build();
    HepPlanner hepPlanner = new HepPlanner(hepProgram);

    final String sql = "select n1.SAL\n"
        + "from EMPNULLABLES_20 n1\n"
        + "where n1.SAL IN (\n"
        + "  select n2.SAL\n"
        + "  from EMPNULLABLES_20 n2\n"
        + "  where n1.SAL = n2.SAL or n1.SAL = 4)";
    sql(sql)
        .withDecorrelation(true)
        .with(hepPlanner)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2195">[CALCITE-2195]
   * AggregateJoinTransposeRule fails to aggregate over unique column</a>. */
  @Test public void testPushAggregateThroughJoin6() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select sum(B.sal)\n"
        + "from sales.emp as A\n"
        + "join (select distinct sal from sales.emp) as B\n"
        + "on A.sal=B.sal\n";
    sql(sql).withPre(preProgram).with(program).check();
  }

  /** SUM is the easiest aggregate function to split. */
  @Test public void testPushAggregateSumThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select e.job,sum(sal)\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2105">[CALCITE-2105]
   * AggregateJoinTransposeRule incorrectly makes a SUM NOT NULL when Aggregate
   * has no group keys</a>. */
  @Test public void testPushAggregateSumWithoutGroupKeyThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select sum(sal)\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.job = d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2108">[CALCITE-2108]
   * AggregateJoinTransposeRule incorrectly splits a SUM0 call when Aggregate
   * has no group keys</a>.
   *
   * <p>Similar to {@link #testPushAggregateSumThroughJoin()},
   * but also uses {@link AggregateReduceFunctionsRule}. */
  @Test public void testPushAggregateSumThroughJoinAfterAggregateReduce() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateReduceFunctionsRule.INSTANCE)
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select sum(sal)\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.job = d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Push a variety of aggregate functions. */
  @Test public void testPushAggregateFunctionsThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select e.job,\n"
        + "  min(sal) as min_sal, min(e.deptno) as min_deptno,\n"
        + "  sum(sal) + 1 as sum_sal_plus, max(sal) as max_sal,\n"
        + "  sum(sal) as sum_sal_2, count(sal) as count_sal,\n"
        + "  count(mgr) as count_mgr\n"
        + "from sales.emp as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Push a aggregate functions into a relation that is unique on the join
   * key. */
  @Test public void testPushAggregateThroughJoinDistinct() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select d.name,\n"
        + "  sum(sal) as sum_sal, count(*) as c\n"
        + "from sales.emp as e\n"
        + "join (select distinct name from sales.dept) as d\n"
        + "  on e.job = d.name\n"
        + "group by d.name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Push count(*) through join, no GROUP BY. */
  @Test public void testPushAggregateSumNoGroup() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql =
        "select count(*) from sales.emp join sales.dept on job = name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testSwapOuterJoin() {
    final HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(JoinCommuteRule.SWAP_OUTER)
        .build();
    checkPlanning(program,
        "select 1 from sales.dept d left outer join sales.emp e"
            + " on d.deptno = e.deptno");
  }


  @Test public void testPushJoinCondDownToProject() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(JoinPushExpressionsRule.INSTANCE)
        .build();
    checkPlanning(program,
        "select d.deptno, e.deptno from sales.dept d, sales.emp e"
            + " where d.deptno + 10 = e.deptno * 2");
  }

  @Test public void testSortJoinTranspose1() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinTransposeRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e left join (\n"
            + "select * from sales.dept d) using (deptno)\n"
            + "order by sal limit 10";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testSortJoinTranspose2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinTransposeRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e right join (\n"
            + "select * from sales.dept d) using (deptno)\n"
            + "order by name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  @Test public void testSortJoinTranspose3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinTransposeRule.INSTANCE)
        .build();
    // This one cannot be pushed down
    final String sql = "select * from sales.emp left join (\n"
        + "select * from sales.dept) using (deptno)\n"
        + "order by sal, name limit 10";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql, true);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-931">[CALCITE-931]
   * Wrong collation trait in SortJoinTransposeRule for right joins</a>. */
  @Test public void testSortJoinTranspose4() {
    // Create a customized test with RelCollation trait in the test cluster.
    Tester tester = new TesterImpl(getDiffRepos(), true, true, false, false,
        null, null) {
      @Override public RelOptPlanner createPlanner() {
        return new MockRelOptPlanner(Contexts.empty()) {
          @Override public List<RelTraitDef> getRelTraitDefs() {
            return ImmutableList.<RelTraitDef>of(RelCollationTraitDef.INSTANCE);
          }
          @Override public RelTraitSet emptyTraitSet() {
            return RelTraitSet.createEmpty().plus(
                RelCollationTraitDef.INSTANCE.getDefault());
          }
        };
      }
    };

    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinTransposeRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e right join (\n"
        + "select * from sales.dept d) using (deptno)\n"
        + "order by name";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1498">[CALCITE-1498]
   * Avoid LIMIT with trivial ORDER BY being pushed through JOIN endlessly</a>. */
  @Test public void testSortJoinTranspose5() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .addRuleInstance(SortJoinTransposeRule.INSTANCE)
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinTransposeRule.INSTANCE)
        .build();
    // SortJoinTransposeRule should not be fired again.
    final String sql = "select * from sales.emp e right join (\n"
        + "select * from sales.dept d) using (deptno)\n"
        + "limit 10";
    checkPlanning(tester, preProgram, new HepPlanner(program), sql, true);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1507">[CALCITE-1507]
   * OFFSET cannot be pushed through a JOIN if the non-preserved side of outer
   * join is not count-preserving</a>. */
  @Test public void testSortJoinTranspose6() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinTransposeRule.INSTANCE)
        .build();
    // This one can be pushed down even if it has an OFFSET, since the dept
    // table is count-preserving against the join condition.
    final String sql = "select d.deptno, empno from sales.dept d\n"
        + "right join sales.emp e using (deptno) limit 10 offset 2";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1507">[CALCITE-1507]
   * OFFSET cannot be pushed through a JOIN if the non-preserved side of outer
   * join is not count-preserving</a>. */
  @Test public void testSortJoinTranspose7() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinTransposeRule.INSTANCE)
        .build();
    // This one cannot be pushed down
    final String sql = "select d.deptno, empno from sales.dept d\n"
        + "left join sales.emp e using (deptno) order by d.deptno offset 1";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .checkUnchanged();
  }

  @Test public void testSortProjectTranspose1() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    // This one can be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as integer) offset 1";
    sql(sql)
        .with(program)
        .check();
  }

  @Test public void testSortProjectTranspose2() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    // This one can be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as double) offset 1";
    sql(sql)
        .with(program)
        .check();
  }

  @Test public void testSortProjectTranspose3() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    // This one cannot be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as varchar(10)) offset 1";
    sql(sql)
        .with(program)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1023">[CALCITE-1023]
   * Planner rule that removes Aggregate keys that are constant</a>. */
  @Test public void testAggregateConstantKeyRule() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .build();
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal";
    checkPlanning(new HepPlanner(program), sql);
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where reduction is not
   * possible because "deptno" is the only key. */
  @Test public void testAggregateConstantKeyRule2() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .build();
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno";
    checkPlanUnchanged(new HepPlanner(program), sql);
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where both keys are
   * constants but only one can be removed. */
  @Test public void testAggregateConstantKeyRule3() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .build();
    final String sql = "select job\n"
        + "from sales.emp\n"
        + "where sal is null and job = 'Clerk'\n"
        + "group by sal, job\n"
        + "having count(*) > 3";
    checkPlanning(new HepPlanner(program), sql);
  }

  @Test public void testReduceExpressionsNot() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();
    checkPlanUnchanged(new HepPlanner(program),
        "select * from (values (false),(true)) as q (col1) where not(col1)");
  }

  private Sql checkSubQuery(String sql) {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SubQueryRemoveRule.PROJECT)
        .addRuleInstance(SubQueryRemoveRule.FILTER)
        .addRuleInstance(SubQueryRemoveRule.JOIN)
        .build();
    return sql(sql).with(new HepPlanner(program)).expand(false);
  }

  /** Tests expanding a sub-query, specifically an uncorrelated scalar
   * sub-query in a project (SELECT clause). */
  @Test public void testExpandProjectScalar() throws Exception {
    final String sql = "select empno,\n"
        + "  (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1493">[CALCITE-1493]
   * Wrong plan for NOT IN correlated queries</a>. */
  @Test public void testWhereNotInCorrelated() {
    final String sql = "select sal from emp\n"
        + "where empno NOT IN (\n"
        + "  select deptno from dept\n"
        + "  where emp.job = dept.name)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testWhereNotInCorrelated2() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno NOT IN\n"
        + "   (select empno from (select ename, empno, sal as r from emp) e2\n"
        + "    where r > 2 and e1.ename= e2.ename)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testAll() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno > ALL (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testSome() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno > SOME (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1546">[CALCITE-1546]
   * Sub-queries connected by OR</a>. */
  @Test public void testWhereOrSubQuery() {
    final String sql = "select * from emp\n"
        + "where sal = 4\n"
        + "or empno NOT IN (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testExpandProjectIn() throws Exception {
    final String sql = "select empno,\n"
        + "  deptno in (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test public void testExpandProjectInNullable() throws Exception {
    final String sql = "with e2 as (\n"
        + "  select empno, case when true then deptno else null end as deptno\n"
        + "  from sales.emp)\n"
        + "select empno,\n"
        + "  deptno in (select deptno from e2 where empno < 20) as d\n"
        + "from e2";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test public void testExpandProjectInComposite() throws Exception {
    final String sql = "select empno, (empno, deptno) in (\n"
        + "    select empno, deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test public void testExpandProjectExists() throws Exception {
    final String sql = "select empno,\n"
        + "  exists (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test public void testExpandFilterScalar() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where (select deptno from sales.emp where empno < 20)\n"
        + " < (select deptno from sales.emp where empno > 100)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test public void testExpandFilterIn() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where deptno in (select deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test public void testExpandFilterInComposite() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where (empno, deptno) in (\n"
        + "  select empno, deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  /** An IN filter that requires full 3-value logic (true, false, unknown). */
  @Test public void testExpandFilterIn3Value() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where empno\n"
        + " < case deptno in (select case when true then deptno else null end\n"
        + "                   from sales.emp where empno < 20)\n"
        + "   when true then 10\n"
        + "   when false then 20\n"
        + "   else 30\n"
        + "   end";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  /** An EXISTS filter that can be converted into true/false. */
  @Test public void testExpandFilterExists() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  /** An EXISTS filter that can be converted into a semi-join. */
  @Test public void testExpandFilterExistsSimple() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  /** An EXISTS filter that can be converted into a semi-join. */
  @Test public void testExpandFilterExistsSimpleAnd() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)\n"
        + "and emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test public void testExpandJoinScalar() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on (select deptno from sales.emp where empno < 20)\n"
        + " < (select deptno from sales.emp where empno > 100)";
    checkSubQuery(sql).check();
  }

  @Ignore("[CALCITE-1045]")
  @Test public void testExpandJoinIn() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on emp.deptno in (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Ignore("[CALCITE-1045]")
  @Test public void testExpandJoinInComposite() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on (emp.empno, dept.deptno) in (\n"
        + "  select empno, deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Test public void testExpandJoinExists() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on exists (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Test public void testDecorrelateExists() throws Exception {
    final String sql = "select * from sales.emp\n"
        + "where EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1511">[CALCITE-1511]
   * AssertionError while decorrelating query with two EXISTS
   * sub-queries</a>. */
  @Test public void testDecorrelateTwoExists() throws Exception {
    final String sql = "select * from sales.emp\n"
        + "where EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)\n"
        + "AND NOT EXISTS (\n"
        + "  select * from emp ee where ee.job = emp.job AND ee.sal=34)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2028">[CALCITE-2028]
   * Un-correlated IN sub-query should be converted into a Join,
   * rather than a Correlate without correlation variables </a>. */
  @Test public void testDecorrelateUncorrelatedInAndCorrelatedExists() throws Exception {
    final String sql = "select * from sales.emp\n"
        + "WHERE job in (\n"
        + "  select job from emp ee where ee.sal=34)"
        + "AND EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)\n";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1537">[CALCITE-1537]
   * Unnecessary project expression in multi-sub-query plan</a>. */
  @Test public void testDecorrelateTwoIn() throws Exception {
    final String sql = "select sal\n"
        + "from sales.emp\n"
        + "where empno IN (\n"
        + "  select deptno from dept where emp.job = dept.name)\n"
        + "AND empno IN (\n"
        + "  select empno from emp e where emp.ename = e.ename)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1045">[CALCITE-1045]
   * Decorrelate sub-queries in Project and Join</a>, with the added
   * complication that there are two sub-queries. */
  @Ignore("[CALCITE-1045]")
  @Test public void testDecorrelateTwoScalar() throws Exception {
    final String sql = "select deptno,\n"
        + "  (select min(1) from emp where empno > d.deptno) as i0,\n"
        + "  (select min(0) from emp\n"
        + "    where deptno = d.deptno and ename = 'SMITH') as i1\n"
        + "from dept as d";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testWhereInJoinCorrelated() {
    final String sql = "select empno from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "where e.sal in (\n"
        + "  select e2.sal from emp as e2 where e2.deptno > e.deptno)";
    checkSubQuery(sql).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1494">[CALCITE-1494]
   * Inefficient plan for correlated sub-queries</a>. In "planAfter", there
   * must be only one scan each of emp and dept. We don't need a separate
   * value-generator for emp.job. */
  @Test public void testWhereInCorrelated() {
    final String sql = "select sal from emp where empno IN (\n"
        + "  select deptno from dept where emp.job = dept.name)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testWhereExpressionInCorrelated() {
    final String sql = "select ename from (\n"
        + "  select ename, deptno, sal + 1 as salPlus from emp) as e\n"
        + "where deptno in (\n"
        + "  select deptno from emp where sal + 1 = e.salPlus)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testWhereExpressionInCorrelated2() {
    final String sql = "select name from (\n"
        + "  select name, deptno, deptno - 10 as deptnoMinus from dept) as d\n"
        + "where deptno in (\n"
        + "  select deptno from emp where sal + 1 = d.deptnoMinus)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test public void testExpandWhereComparisonCorrelated() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp as e\n"
        + "where sal = (\n"
        + "  select max(sal) from sales.emp e2 where e2.empno = e.empno)";
    checkSubQuery(sql).check();
  }

  @Test public void testCustomColumnResolvingInNonCorrelatedSubQuery() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 in (\n"
        + "  select f1.c0 from struct.t t2)";
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SubQueryRemoveRule.PROJECT)
        .addRuleInstance(SubQueryRemoveRule.FILTER)
        .addRuleInstance(SubQueryRemoveRule.JOIN)
        .build();
    sql(sql)
        .withTrim(true)
        .expand(false)
        .with(program)
        .check();
  }

  @Test public void testCustomColumnResolvingInCorrelatedSubQuery() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 = (\n"
        + "  select max(f1.c0) from struct.t t2 where t1.k0 = t2.k0)";
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SubQueryRemoveRule.PROJECT)
        .addRuleInstance(SubQueryRemoveRule.FILTER)
        .addRuleInstance(SubQueryRemoveRule.JOIN)
        .build();
    sql(sql)
        .withTrim(true)
        .expand(false)
        .with(program)
        .check();
  }

  @Test public void testCustomColumnResolvingInCorrelatedSubQuery2() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 in (\n"
        + "  select f1.c0 from struct.t t2 where t1.c2 = t2.c2)";
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SubQueryRemoveRule.PROJECT)
        .addRuleInstance(SubQueryRemoveRule.FILTER)
        .addRuleInstance(SubQueryRemoveRule.JOIN)
        .build();
    sql(sql)
        .withTrim(true)
        .expand(false)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-434">[CALCITE-434]
   * Converting predicates on date dimension columns into date ranges</a>,
   * specifically a rule that converts {@code EXTRACT(YEAR FROM ...) = constant}
   * to a range. */
  @Test public void testExtractYearToRange() {
    final String sql = "select *\n"
        + "from sales.emp_b as e\n"
        + "where extract(year from birthdate) = 2014";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(DateRangeRules.FILTER_INSTANCE)
        .build();
    final Context context =
        Contexts.of(new CalciteConnectionConfigImpl(new Properties()));
    sql(sql).with(program).withContext(context).check();
  }

  @Test public void testExtractYearMonthToRange() {
    final String sql = "select *\n"
        + "from sales.emp_b as e\n"
        + "where extract(year from birthdate) = 2014"
        + "and extract(month from birthdate) = 4";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(DateRangeRules.FILTER_INSTANCE)
        .build();
    final Context context =
        Contexts.of(new CalciteConnectionConfigImpl(new Properties()));
    sql(sql).with(program).withContext(context).check();
  }

}

// End RelOptRulesTest.java
