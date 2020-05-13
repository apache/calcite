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
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.AggregateCaseToFilterRule;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rel.rules.AggregateFilterTransposeRule;
import org.apache.calcite.rel.rules.AggregateJoinJoinRemoveRule;
import org.apache.calcite.rel.rules.AggregateJoinRemoveRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rel.rules.AggregateValuesRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.ExchangeRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterRemoveIsNotDistinctFromRule;
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
import org.apache.calcite.rel.rules.ProjectCorrelateTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinJoinRemoveRule;
import org.apache.calcite.rel.rules.ProjectJoinRemoveRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.ReduceDecimalsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinJoinTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortJoinCopyRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.catalog.MockCatalogReader;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Predicate;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.operandJ;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
class RelOptRulesTest extends RelOptTestBase {
  //~ Methods ----------------------------------------------------------------

  private final PushProjector.ExprCondition skipItem = expr ->
      expr instanceof RexCall
          && "item".equalsIgnoreCase(((RexCall) expr).getOperator().getName());

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(RelOptRulesTest.class);
  }

  @Test void testReduceNot() {
    HepProgram preProgram = new HepProgramBuilder()
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.FILTER_INSTANCE);

    final String sql = "select *\n"
        + "from (select (case when sal > 1000 then null else false end) as caseCol from emp)\n"
        + "where NOT(caseCol)";
    sql(sql).withPre(preProgram)
        .with(hepPlanner)
        .checkUnchanged();
  }

  @Test void testReduceNestedCaseWhen() {
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
    sql(sql).withPre(preProgram)
        .with(hepPlanner)
        .check();
  }

  @Test void testDigestOfApproximateDistinctAggregateCall() {
    HepProgram preProgram = new HepProgramBuilder()
            .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(AggregateProjectMergeRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(AggregateProjectMergeRule.INSTANCE);

    final String sql = "select *\n"
            + "from (\n"
            + "select deptno, count(distinct empno) from emp group by deptno\n"
            + "union all\n"
            + "select deptno, approx_count_distinct(empno) from emp group by deptno)";
    sql(sql).withPre(preProgram)
            .with(hepPlanner)
            .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1479">[CALCITE-1479]
   * AssertionError in ReduceExpressionsRule on multi-column IN
   * sub-query</a>. */
  @Test void testReduceCompositeInSubQuery() {
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2865">[CALCITE-2865]
   * FilterProjectTransposeRule generates wrong traitSet when copyFilter/Project is true</a>. */
  @Test void testFilterProjectTransposeRule() {
    List<RelOptRule> rules = Arrays.asList(
            FilterProjectTransposeRule.INSTANCE, // default: copyFilter=true, copyProject=true
            new FilterProjectTransposeRule(Filter.class, Project.class,
                false, false, RelFactories.LOGICAL_BUILDER));

    for (RelOptRule rule : rules) {
      RelBuilder b = RelBuilder.create(RelBuilderTest.config().build());
      RelNode in = b
              .scan("EMP")
              .sort(-4) // salary desc
              .project(b.field(3)) // salary
              .filter(b.equals(b.field(0), b.literal(11500))) // salary = 11500
              .build();
      HepProgram program = new HepProgramBuilder()
              .addRuleInstance(rule)
              .build();
      HepPlanner hepPlanner = new HepPlanner(program);
      hepPlanner.setRoot(in);
      RelNode result = hepPlanner.findBestExp();

      // Verify LogicalFilter traitSet (must be [3 DESC])
      RelNode filter = result.getInput(0);
      RelCollation collation = filter.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
      assertNotNull(collation);
      List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
      assertEquals(1, fieldCollations.size());
      RelFieldCollation fieldCollation = fieldCollations.get(0);
      assertEquals(3, fieldCollation.getFieldIndex());
      assertEquals(RelFieldCollation.Direction.DESCENDING, fieldCollation.getDirection());
    }
  }

  @Test void testReduceOrCaseWhen() {
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
    sql(sql).withPre(preProgram)
        .with(hepPlanner)
        .check();
  }

  @Test void testReduceNullableCase() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);

    final String sql = "SELECT CASE WHEN 1=2 "
        + "THEN cast((values(1)) as integer) "
        + "ELSE 2 end from (values(1))";
    sql(sql).with(hepPlanner).checkUnchanged();
  }

  @Test void testReduceNullableCase2() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);

    final String sql = "SELECT deptno, ename, CASE WHEN 1=2 "
        + "THEN substring(ename, 1, cast(2 as int)) ELSE NULL end from emp"
        + " group by deptno, ename, case when 1=2 then substring(ename,1, cast(2 as int))  else null end";
    sql(sql).with(hepPlanner).checkUnchanged();
  }

  @Test void testProjectToWindowRuleForMultipleWindows() {
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
    sql(sql).withPre(preProgram)
        .with(hepPlanner)
        .check();
  }

  @Test void testUnionToDistinctRule() {
    final String sql = "select * from dept union select * from dept";
    sql(sql).withRule(UnionToDistinctRule.INSTANCE).check();
  }

  @Test void testExtractJoinFilterRule() {
    final String sql = "select 1 from emp inner join dept on emp.deptno=dept.deptno";
    sql(sql).withRule(JoinExtractFilterRule.INSTANCE).check();
  }

  @Test void testNotPushExpression() {
    final String sql = "select 1 from emp inner join dept\n"
        + "on emp.deptno=dept.deptno and emp.ename is not null";
    sql(sql).withRule(JoinPushExpressionsRule.INSTANCE)
        .checkUnchanged();
  }

  @Test void testAddRedundantSemiJoinRule() {
    final String sql = "select 1 from emp inner join dept on emp.deptno = dept.deptno";
    sql(sql).withRule(JoinAddRedundantSemiJoinRule.INSTANCE).check();
  }

  @Test void testStrengthenJoinType() {
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
        + "from dept left join emp on dept.deptno = emp.deptno\n"
        + "where emp.deptno is not null and emp.sal > 100";
    sql(sql)
        .withDecorrelation(true)
        .withTrim(true)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3170">[CALCITE-3170]
   * ANTI join on conditions push down generates wrong plan</a>. */
  @Test void testCanNotPushAntiJoinConditionsToLeft() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // not in (select dept.deptno from dept where emp.deptno > 20)
    RelNode left = relBuilder.scan("EMP").build();
    RelNode right = relBuilder.scan("DEPT").build();
    RelNode relNode = relBuilder.push(left)
        .push(right)
        .antiJoin(
            relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")),
            relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
            RexInputRef.of(0, left.getRowType()),
            relBuilder.literal(20)))
        .project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.JOIN)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testCanNotPushAntiJoinConditionsToRight() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // not in (select dept.deptno from dept where dept.dname = 'ddd')
    RelNode relNode = relBuilder.scan("EMP")
        .scan("DEPT")
        .antiJoin(
            relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")),
            relBuilder.equals(relBuilder.field(2, 1, "DNAME"),
                relBuilder.literal("ddd")))
        .project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.JOIN)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3171">[CALCITE-3171]
   * SemiJoin on conditions push down throws IndexOutOfBoundsException</a>. */
  @Test void testPushSemiJoinConditionsToLeft() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // in (select dept.deptno from dept where emp.empno > 20)
    RelNode left = relBuilder.scan("EMP").build();
    RelNode right = relBuilder.scan("DEPT").build();
    RelNode relNode = relBuilder.push(left)
        .push(right)
        .semiJoin(
            relBuilder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")),
            relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
                RexInputRef.of(0, left.getRowType()),
                relBuilder.literal(20)))
        .project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(JoinPushExpressionsRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3979">[CALCITE-3979]
   * ReduceExpressionsRule might have removed CAST expression(s) incorrectly</a>. */
  @Test public void testCastRemove() throws Exception {
    final String sql = "select\n"
        + "case when cast(ename as double) < 5 then 0.0\n"
        + "     else coalesce(cast(ename as double), 1.0)\n"
        + "     end as t\n"
        + " from (\n"
        + "       select\n"
        + "          case when ename > 'abc' then ename\n"
        + "               else null\n"
        + "               end as ename from emp\n"
        + " )";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3887">[CALCITE-3887]
   * Filter and Join conditions may not need to retain nullability during simplifications</a>. */
  @Test void testPushSemiJoinConditions() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode left = relBuilder.scan("EMP")
        .project(
            relBuilder.field("DEPTNO"),
            relBuilder.field("ENAME"))
        .build();
    RelNode right = relBuilder.scan("DEPT")
        .project(
            relBuilder.field("DEPTNO"),
            relBuilder.field("DNAME"))
        .build();

    relBuilder.push(left).push(right);

    RexInputRef ref1 = relBuilder.field(2, 0, "DEPTNO");
    RexInputRef ref2 = relBuilder.field(2, 1, "DEPTNO");
    RexInputRef ref3 = relBuilder.field(2, 0, "ENAME");
    RexInputRef ref4 = relBuilder.field(2, 1, "DNAME");

    // ref1 IS NOT DISTINCT FROM ref2
    RexCall cond1 = (RexCall) relBuilder.call(
        SqlStdOperatorTable.OR,
        relBuilder.call(SqlStdOperatorTable.EQUALS, ref1, ref2),
        relBuilder.call(SqlStdOperatorTable.AND,
            relBuilder.call(SqlStdOperatorTable.IS_NULL, ref1),
            relBuilder.call(SqlStdOperatorTable.IS_NULL, ref2)));

    // ref3 IS NOT DISTINCT FROM ref4
    RexCall cond2 = (RexCall) relBuilder.call(
        SqlStdOperatorTable.OR,
        relBuilder.call(SqlStdOperatorTable.EQUALS, ref3, ref4),
        relBuilder.call(SqlStdOperatorTable.AND,
            relBuilder.call(SqlStdOperatorTable.IS_NULL, ref3),
            relBuilder.call(SqlStdOperatorTable.IS_NULL, ref4)));

    RexNode cond = relBuilder.call(SqlStdOperatorTable.AND, cond1, cond2);
    RelNode relNode = relBuilder.semiJoin(cond)
        .project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(JoinPushExpressionsRule.INSTANCE)
        .addRuleInstance(SemiJoinProjectTransposeRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testFullOuterJoinSimplificationToLeftOuter() {
    final String sql = "select 1 from sales.dept d full outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where d.name = 'Charlie'";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN).check();
  }

  @Test void testFullOuterJoinSimplificationToRightOuter() {
    final String sql = "select 1 from sales.dept d full outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where e.sal > 100";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN).check();
  }

  @Test void testFullOuterJoinSimplificationToInner() {
    final String sql = "select 1 from sales.dept d full outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where d.name = 'Charlie' and e.sal > 100";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN).check();
  }

  @Test void testLeftOuterJoinSimplificationToInner() {
    final String sql = "select 1 from sales.dept d left outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where e.sal > 100";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN).check();
  }

  @Test void testRightOuterJoinSimplificationToInner() {
    final String sql = "select 1 from sales.dept d right outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where d.name = 'Charlie'";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN).check();
  }

  @Test void testPushAboveFiltersIntoInnerJoinCondition() {
    final String sql = ""
        + "select * from sales.dept d inner join sales.emp e\n"
        + "on d.deptno = e.deptno and d.deptno > e.mgr\n"
        + "where d.deptno > e.mgr";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3225">[CALCITE-3225]
   * JoinToMultiJoinRule should not match SEMI/ANTI LogicalJoin</a>. */
  @Test void testJoinToMultiJoinDoesNotMatchSemiJoin() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from
    // (select * from emp join dept ON emp.deptno = emp.deptno) t
    // where emp.job in (select job from bonus)
    RelNode left = relBuilder.scan("EMP").build();
    RelNode right = relBuilder.scan("DEPT").build();
    RelNode semiRight = relBuilder.scan("BONUS").build();
    RelNode relNode = relBuilder.push(left)
                                .push(right)
                                .join(
                                    JoinRelType.INNER,
                                    relBuilder.call(SqlStdOperatorTable.EQUALS,
                                                    relBuilder.field(2, 0, "DEPTNO"),
                                                    relBuilder.field(2, 1, "DEPTNO")))
                                .push(semiRight)
                                .semiJoin(
                                    relBuilder.call(SqlStdOperatorTable.EQUALS,
                                                    relBuilder.field(2, 0, "JOB"),
                                                    relBuilder.field(2, 1, "JOB")))
                                .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3225">[CALCITE-3225]
   * JoinToMultiJoinRule should not match SEMI/ANTI LogicalJoin</a>. */
  @Test void testJoinToMultiJoinDoesNotMatchAntiJoin() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select * from
    // (select * from emp join dept ON emp.deptno = emp.deptno) t
    // where not exists (select job from bonus where emp.job = bonus.job)
    RelNode left = relBuilder.scan("EMP").build();
    RelNode right = relBuilder.scan("DEPT").build();
    RelNode antiRight = relBuilder.scan("BONUS").build();
    RelNode relNode = relBuilder.push(left)
                                .push(right)
                                .join(
                                    JoinRelType.INNER,
                                    relBuilder.call(SqlStdOperatorTable.EQUALS,
                                                    relBuilder.field(2, 0, "DEPTNO"),
                                                    relBuilder.field(2, 1, "DEPTNO")))
                                .push(antiRight)
                                .antiJoin(
                                    relBuilder.call(SqlStdOperatorTable.EQUALS,
                                                    relBuilder.field(2, 0, "JOB"),
                                                    relBuilder.field(2, 1, "JOB")))
                                .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testPushFilterPastAgg() {
    final String sql = "select dname, c from\n"
        + "(select name dname, count(*) as c from dept group by name) t\n"
        + " where dname = 'Charlie'";
    sql(sql).withRule(FilterAggregateTransposeRule.INSTANCE).check();
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
    Sql sql = sql("${sql}").withPre(preProgram)
        .with(program);
    if (unchanged) {
      sql.checkUnchanged();
    } else {
      sql.check();
    }
  }

  @Test void testPushFilterPastAggWithGroupingSets1() throws Exception {
    basePushFilterPastAggWithGroupingSets(true);
  }

  @Test void testPushFilterPastAggWithGroupingSets2() throws Exception {
    basePushFilterPastAggWithGroupingSets(false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-434">[CALCITE-434]
   * FilterAggregateTransposeRule loses conditions that cannot be pushed</a>. */
  @Test void testPushFilterPastAggTwo() {
    final String sql = "select dept1.c1 from (\n"
        + "select dept.name as c1, count(*) as c2\n"
        + "from dept where dept.name > 'b' group by dept.name) dept1\n"
        + "where dept1.c1 > 'c' and (dept1.c2 > 30 or dept1.c1 < 'z')";
    sql(sql).withRule(FilterAggregateTransposeRule.INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-799">[CALCITE-799]
   * Incorrect result for {@code HAVING count(*) > 1}</a>. */
  @Test void testPushFilterPastAggThree() {
    final String sql = "select deptno from emp\n"
        + "group by deptno having count(*) > 1";
    sql(sql).withRule(FilterAggregateTransposeRule.INSTANCE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1109">[CALCITE-1109]
   * FilterAggregateTransposeRule pushes down incorrect condition</a>. */
  @Test void testPushFilterPastAggFour() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
            .addRuleInstance(AggregateFilterTransposeRule.INSTANCE)
            .build();
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(FilterAggregateTransposeRule.INSTANCE)
            .build();
    final String sql = "select emp.deptno, count(*) from emp where emp.sal > '12'\n"
        + "group by emp.deptno";
    sql(sql)
        .withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-448">[CALCITE-448]
   * FilterIntoJoinRule creates filters containing invalid RexInputRef</a>. */
  @Test void testPushFilterPastProject() {
    final HepProgram preProgram =
        HepProgram.builder()
            .addRuleInstance(ProjectMergeRule.INSTANCE)
            .build();
    final FilterJoinRule.Predicate predicate =
        (join, joinType, exp) -> joinType != JoinRelType.INNER;
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
    sql(sql).withPre(preProgram)
        .with(program)
        .check();
  }

  @Test void testJoinProjectTranspose1() {
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
    sql(sql).withPre(preProgram)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1338">[CALCITE-1338]
   * JoinProjectTransposeRule should not pull a literal above the
   * null-generating side of a join</a>. */
  @Test void testJoinProjectTranspose2() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name, 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the left project of right join has literal. */
  @Test void testJoinProjectTranspose3() {
    final String sql = "select *\n"
        + "from (select name, 1 from dept) as a\n"
        + "right join dept b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the right project of left join has not-strong
   * expression {@code y is not null}. */
  @Test void testJoinProjectTranspose4() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select x name, y is not null from\n"
        + "(values (2, cast(null as integer)), (2, 1)) as t(x, y)) b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since the right project of left join has not-strong
   * expression {@code 1 + 1}. */
  @Test void testJoinProjectTranspose5() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name, 1 + 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should not transpose since both the left project and right project have
   * literal. */
  @Test void testJoinProjectTranspose6() {
    final String sql = "select *\n"
        + "from (select name, 1 from dept) a\n"
        + "full join (select name, 1 from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER)
        .checkUnchanged();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * Should transpose since all expressions in the right project of left join
   * are strong. */
  @Test void testJoinProjectTranspose7() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (select name from dept) as b\n"
        + " on a.name = b.name";
    sql(sql)
        .withRule(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER)
        .check();
  }

  /** As {@link #testJoinProjectTranspose2()};
   * should transpose since all expressions including
   * {@code deptno > 10 and cast(null as boolean)} in the right project of left
   * join are strong. */
  @Test void testJoinProjectTranspose8() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "left join (\n"
        + "  select name, deptno > 10 and cast(null as boolean)\n"
        + "  from dept) as b\n"
        + "on a.name = b.name";
    sql(sql)
        .withRule(JoinProjectTransposeRule.RIGHT_PROJECT_INCLUDE_OUTER)
        .check();
  }

  @Test void testJoinProjectTransposeWindow() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "join (select rank() over (order by name) as r, 1 + 1 from dept) as b\n"
        + "on a.name = b.r";
    sql(sql)
        .withRule(JoinProjectTransposeRule.BOTH_PROJECT)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3353">[CALCITE-3353]
   * ProjectJoinTransposeRule caused AssertionError when creating a new Join</a>.
   */
  @Test void testProjectJoinTransposeWithMergeJoin() {
    ProjectJoinTransposeRule testRule = new ProjectJoinTransposeRule(
            Project.class, Join.class, expr -> !(expr instanceof RexOver),
            RelFactories.LOGICAL_BUILDER);
    ImmutableList<RelOptRule> commonRules = ImmutableList.of(
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_VALUES_RULE);
    final RuleSet rules = RuleSets.ofList(ImmutableList.<RelOptRule>builder()
            .addAll(commonRules)
            .add(ProjectJoinTransposeRule.INSTANCE)
            .build());
    final RuleSet testRules = RuleSets.ofList(ImmutableList.<RelOptRule>builder()
            .addAll(commonRules)
            .add(testRule).build());

    FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
            .build();

    RelBuilder builder = RelBuilder.create(config);
    RelNode logicalPlan = builder
            .values(new String[]{"id", "name"}, "1", "anna", "2", "bob", "3", "tom")
            .values(new String[]{"name", "age"}, "anna", "14", "bob", "17", "tom", "22")
            .join(JoinRelType.INNER, "name")
            .project(builder.field(3))
            .build();

    RelTraitSet desiredTraits = logicalPlan.getTraitSet()
            .replace(EnumerableConvention.INSTANCE);
    RelOptPlanner planner = logicalPlan.getCluster().getPlanner();
    RelNode enumerablePlan1 = Programs.of(rules).run(planner, logicalPlan,
            desiredTraits, ImmutableList.of(), ImmutableList.of());
    RelNode enumerablePlan2 = Programs.of(testRules).run(planner, logicalPlan,
            desiredTraits, ImmutableList.of(), ImmutableList.of());
    assertEquals(RelOptUtil.toString(enumerablePlan1), RelOptUtil.toString(enumerablePlan2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-889">[CALCITE-889]
   * Implement SortUnionTransposeRule</a>. */
  @Test void testSortUnionTranspose() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(SortUnionTransposeRule.INSTANCE)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 10";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-889">[CALCITE-889]
   * Implement SortUnionTransposeRule</a>. */
  @Test void testSortUnionTranspose2() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(SortUnionTransposeRule.MATCH_NULL_FETCH)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-987">[CALCITE-987]
   * Push limit 0 will result in an infinite loop</a>. */
  @Test void testSortUnionTranspose3() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(SortUnionTransposeRule.MATCH_NULL_FETCH)
            .build();
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 0";
    sql(sql).with(program).check();
  }

  @Test void testSortRemovalAllKeysConstant() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortRemoveConstantKeysRule.INSTANCE)
        .build();
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal\n"
        + "order by deptno desc nulls last";
    sql(sql).with(program).check();
  }

  @Test void testSortRemovalOneKeyConstant() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortRemoveConstantKeysRule.INSTANCE)
        .build();
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal\n"
        + "order by deptno, sal desc nulls first";
    sql(sql).with(program).check();
  }

  @Test void testSemiJoinRuleExists() {
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

  @Test void testSemiJoinRule() {
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
  @Test void testSemiJoinRuleRight() {
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
  @Test void testSemiJoinRuleFull() {
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
  @Test void testSemiJoinRuleLeft() {
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
  @Test void testPushFilterThroughSemiJoin() {
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
  @Test void testSemiJoinReduceConstants() {
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
        .check();
  }

  @Test void testSemiJoinTrim() throws Exception {
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

  @Test void testReduceAverage() {
    final String sql = "select name, max(name), avg(deptno), min(name)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(AggregateReduceFunctionsRule.INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1621">[CALCITE-1621]
   * Adding a cast around the null literal in aggregate rules</a>. */
  @Test void testCastInAggregateReduceFunctions() {
    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(AggregateReduceFunctionsRule.INSTANCE)
            .build();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),\n"
        + "stddev_samp(deptno),var_pop(deptno), var_samp(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountWithoutGroupBy() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select max(deptno), count(distinct ename)\n"
        + "from sales.emp";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCount1() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, count(distinct ename)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCount2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, count(distinct ename), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1293">[CALCITE-1293]
   * Bad code generated when argument to COUNT(DISTINCT) is a # GROUP BY
   * column</a>. */
  @Test void testDistinctCount3() {
    final String sql = "select count(distinct deptno), sum(sal)"
        + " from sales.emp group by deptno";
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .build();
    sql(sql).with(program).check();
  }

  /** Tests implementing multiple distinct count the old way, using a join. */
  @Test void testDistinctCountMultipleViaJoin() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, count(distinct ename), count(distinct job, ename),\n"
        + "count(distinct deptno, job), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  /** Tests implementing multiple distinct count the new way, using GROUPING
   *  SETS. */
  @Test void testDistinctCountMultiple() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, count(distinct ename), count(distinct job)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountMultipleNoGroup() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select count(distinct ename), count(distinct job)\n"
        + "from sales.emp";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountMixedJoin() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, count(distinct ename), count(distinct job, ename),\n"
        + "count(distinct deptno, job), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountMixed() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, count(distinct deptno, job) as cddj, sum(sal) as s\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountMixed2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, count(distinct ename) as cde,\n"
        + "count(distinct job, ename) as cdje,\n"
        + "count(distinct deptno, job) as cddj,\n"
        + "sum(sal) as s\n"
        + "from sales.emp group by deptno";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountGroupingSets1() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, job, count(distinct ename)\n"
        + "from sales.emp group by rollup(deptno,job)";
    sql(sql).with(program).check();
  }

  @Test void testDistinctCountGroupingSets2() {
    final HepProgram program = HepProgram.builder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, job, count(distinct ename), sum(sal)\n"
        + "from sales.emp group by rollup(deptno,job)";
    sql(sql).with(program).check();
  }

  @Test void testDistinctNonDistinctAggregates() {
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
  @Test void testCastInAggregateExpandDistinctAggregatesRule() {
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
  @Test void testDistinctNonDistinctAggregatesWithGrouping1() {
    final String sql = "SELECT deptno,\n"
        + "  SUM(deptno), SUM(DISTINCT sal), MAX(deptno), MAX(comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctNonDistinctAggregatesWithGrouping2() {
    final String sql = "SELECT deptno, COUNT(deptno), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctNonDistinctTwoAggregatesWithGrouping() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithGrouping() {
    final String sql = "SELECT sal, SUM(comm), MIN(comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY sal";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testRemoveDistinctOnAgg() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateRemoveRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "SELECT empno, SUM(distinct sal), MIN(sal), "
        + "MIN(distinct sal), MAX(distinct sal), "
        + "bit_and(distinct sal), bit_or(sal), count(distinct sal) "
        + "from sales.emp group by empno, deptno\n";
    sql(sql).with(program).check();
  }

  @Test void testMultipleDistinctWithGrouping() {
    final String sql = "SELECT sal, SUM(comm), AVG(DISTINCT comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY sal";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithMultipleInputs() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), COUNT(DISTINCT sal, comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithMultipleInputsAndGroupby() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), COUNT(DISTINCT sal, deptno, comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.JOIN)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithFilterWithoutGroupBy() {
    final String sql = "SELECT SUM(comm), COUNT(DISTINCT sal) FILTER (WHERE sal > 1000)\n"
        + "FROM emp";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithDiffFiltersAndSameGroupSet() {
    final String sql = "SELECT COUNT(DISTINCT c) FILTER (WHERE d),\n"
        + "COUNT(DISTINCT d) FILTER (WHERE c)\n"
        + "FROM (select sal > 1000 is true as c, sal < 500 is true as d, comm from emp)";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testDistinctWithFilterAndGroupBy() {
    final String sql = "SELECT deptno, SUM(comm), COUNT(DISTINCT sal) FILTER (WHERE sal > 1000)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateExpandDistinctAggregatesRule.INSTANCE)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testPushProjectPastFilter() {
    final String sql = "select empno + deptno from emp where sal = 10 * comm\n"
        + "and upper(ename) = 'FOO'";
    sql(sql).withRule(ProjectFilterTransposeRule.INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1778">[CALCITE-1778]
   * Query with "WHERE CASE" throws AssertionError "Cast for just nullability
   * not allowed"</a>. */
  @Test void testPushProjectPastFilter2() {
    final String sql = "select count(*)\n"
        + "from emp\n"
        + "where case when mgr < 10 then true else false end";
    sql(sql).withRule(ProjectFilterTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastJoin() {
    final String sql = "select e.sal + b.comm from emp e inner join bonus b\n"
        + "on e.ename = b.ename and e.deptno = 10";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3004">[CALCITE-3004]
   * Should not push over past union but its operands can since setop
   * will affect row count</a>. */
  @Test void testProjectSetOpTranspose() {
    final String sql = "select job, sum(sal + 100) over (partition by deptno) from\n"
        + "(select * from emp e1 union all select * from emp e2)";
    sql(sql).withRule(ProjectSetOpTransposeRule.INSTANCE).check();
  }

  @Test void testProjectCorrelateTransposeDynamic() {
    ProjectCorrelateTransposeRule customPCTrans =
        new ProjectCorrelateTransposeRule(skipItem, RelFactories.LOGICAL_BUILDER);

    HepProgramBuilder programBuilder = HepProgram.builder()
        .addRuleInstance(customPCTrans);

    String query = "select t1.c_nationkey, t2.a as fake_col2 "
        + "from SALES.CUSTOMER as t1, "
        + "unnest(t1.fake_col) as t2(a)";
    sql(query).withTester(t -> createDynamicTester())
        .with(programBuilder.build())
        .checkUnchanged();
  }

  @Test void testProjectCorrelateTransposeRuleLeftCorrelate() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1 "
        + "where exists (select empno, deptno from dept d2 where e1.deptno = d2.deptno)";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)
        .addRuleInstance(ProjectCorrelateTransposeRule.INSTANCE)
        .build();
    sql(sql)
        .withDecorrelation(false)
        .expand(true)
        .with(program)
        .check();
  }

  @Test void testProjectCorrelateTransposeRuleSemiCorrelate() {
    RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode left = relBuilder
        .values(new String[]{"f", "f2"}, "1", "2").build();

    CorrelationId correlationId = new CorrelationId(0);
    RexNode rexCorrel =
        relBuilder.getRexBuilder().makeCorrel(
            left.getRowType(),
            correlationId);

    RelNode right = relBuilder
        .values(new String[]{"f3", "f4"}, "1", "2")
        .project(relBuilder.field(0),
            relBuilder.getRexBuilder()
                .makeFieldAccess(rexCorrel, 0))
        .build();
    LogicalCorrelate correlate = new LogicalCorrelate(left.getCluster(),
        left.getTraitSet(), left, right, correlationId,
        ImmutableBitSet.of(0), JoinRelType.SEMI);

    relBuilder.push(correlate);
    RelNode relNode = relBuilder.project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectCorrelateTransposeRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testProjectCorrelateTransposeRuleAntiCorrelate() {
    RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode left = relBuilder
        .values(new String[]{"f", "f2"}, "1", "2").build();

    CorrelationId correlationId = new CorrelationId(0);
    RexNode rexCorrel =
        relBuilder.getRexBuilder().makeCorrel(
            left.getRowType(),
            correlationId);

    RelNode right = relBuilder
        .values(new String[]{"f3", "f4"}, "1", "2")
        .project(relBuilder.field(0),
            relBuilder.getRexBuilder().makeFieldAccess(rexCorrel, 0)).build();
    LogicalCorrelate correlate = new LogicalCorrelate(left.getCluster(),
        left.getTraitSet(), left, right, correlationId,
        ImmutableBitSet.of(0), JoinRelType.ANTI);

    relBuilder.push(correlate);
    RelNode relNode = relBuilder.project(relBuilder.field(0))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectCorrelateTransposeRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testProjectCorrelateTransposeWithExprCond() {
    ProjectCorrelateTransposeRule customPCTrans =
        new ProjectCorrelateTransposeRule(skipItem, RelFactories.LOGICAL_BUILDER);

    final String sql = "select t1.name, t2.ename\n"
        + "from DEPT_NESTED as t1,\n"
        + "unnest(t1.employees) as t2";
    sql(sql).withRule(customPCTrans).check();
  }

  @Test void testSwapOuterJoinFieldAccess() {
    HepProgram preProgram = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(JoinProjectTransposeRule.LEFT_PROJECT_INCLUDE_OUTER)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(JoinCommuteRule.SWAP_OUTER)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select t1.name, e.ename\n"
        + "from DEPT_NESTED as t1 left outer join sales.emp e\n"
        + " on t1.skill.type = e.job";
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test void testProjectCorrelateTranspose() {
    ProjectCorrelateTransposeRule customPCTrans =
        new ProjectCorrelateTransposeRule(expr -> true,
            RelFactories.LOGICAL_BUILDER);
    final String sql = "select t1.name, t2.ename\n"
        + "from DEPT_NESTED as t1,\n"
        + "unnest(t1.employees) as t2";
    sql(sql).withRule(customPCTrans).check();
  }

  /** As {@link #testProjectSetOpTranspose()};
   * should not push over past correlate but its operands can since correlate
   * will affect row count. */
  @Test void testProjectCorrelateTransposeWithOver() {
    final String sql = "select sum(t1.deptno + 1) over (partition by t1.name),\n"
        + "count(t2.empno) over ()\n"
        + "from DEPT_NESTED as t1,\n"
        + "unnest(t1.employees) as t2";
    sql(sql).withRule(ProjectCorrelateTransposeRule.INSTANCE).check();
  }

  /** Tests that the default instance of {@link FilterProjectTransposeRule}
   * does not push a Filter that contains a correlating variable.
   *
   * @see #testFilterProjectTranspose() */
  @Test void testFilterProjectTransposePreventedByCorrelation() {
    final String sql = "SELECT e.empno\n"
        + "FROM emp as e\n"
        + "WHERE exists (\n"
        + "  SELECT *\n"
        + "  FROM (\n"
        + "    SELECT deptno * 2 AS twiceDeptno\n"
        + "    FROM dept) AS d\n"
        + "  WHERE e.deptno = d.twiceDeptno)";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .build();
    sql(sql)
        .withDecorrelation(false)
        .expand(true)
        .with(program)
        .checkUnchanged();
  }

  /** Tests a variant of {@link FilterProjectTransposeRule}
   * that pushes a Filter that contains a correlating variable. */
  @Test void testFilterProjectTranspose() {
    final String sql = "SELECT e.empno\n"
        + "FROM emp as e\n"
        + "WHERE exists (\n"
        + "  SELECT *\n"
        + "  FROM (\n"
        + "    SELECT deptno * 2 AS twiceDeptno\n"
        + "    FROM dept) AS d\n"
        + "  WHERE e.deptno = d.twiceDeptno)";
    final FilterProjectTransposeRule filterProjectTransposeRule =
        new FilterProjectTransposeRule(Filter.class, filter -> true,
            Project.class, project -> true, true, true,
            RelFactories.LOGICAL_BUILDER);
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(filterProjectTransposeRule)
        .build();
    sql(sql)
        .withDecorrelation(false)
        .expand(true)
        .with(program)
        .check();
  }

  private static final String NOT_STRONG_EXPR =
      "case when e.sal < 11 then 11 else -1 * e.sal end";

  private static final String STRONG_EXPR =
      "case when e.sal < 11 then -1 * e.sal else e.sal end";

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1753">[CALCITE-1753]
   * PushProjector should only preserve expressions if the expression is strong
   * when pushing into the nullable-side of outer join</a>. */
  @Test void testPushProjectPastInnerJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e inner join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastInnerJoinStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from emp e inner join bonus b on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastLeftJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e left outer join bonus b on e.ename = b.ename\n"
        + "group by case when e.sal < 11 then 11 else -1 * e.sal end";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastLeftJoinSwap() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from bonus b left outer join emp e on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastLeftJoinSwapStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from bonus b left outer join emp e on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastRightJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e right outer join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastRightJoinStrong() {
    final String sql = "select count(*),\n"
        + " case when e.sal < 11 then -1 * e.sal else e.sal end\n"
        + "from emp e right outer join bonus b on e.ename = b.ename\n"
        + "group by case when e.sal < 11 then -1 * e.sal else e.sal end";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastRightJoinSwap() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from bonus b right outer join emp e on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastRightJoinSwapStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from bonus b right outer join emp e on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastFullJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e full outer join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastFullJoinStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from emp e full outer join bonus b on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2343">[CALCITE-2343]
   * Should not push over whose columns are all from left child past join since
   * join will affect row count.</a>. */
  @Test void testPushProjectWithOverPastJoin1() {
    final String sql = "select e.sal + b.comm,\n"
        + "count(e.empno) over (partition by e.deptno)\n"
        + "from emp e join bonus b\n"
        + "on e.ename = b.ename and e.deptno = 10";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  /** As {@link #testPushProjectWithOverPastJoin1()};
   * should not push over whose columns are all from right child past join since
   * join will affect row count. */
  @Test void testPushProjectWithOverPastJoin2() {
    final String sql = "select e.sal + b.comm,\n"
        + "count(b.sal) over (partition by b.job)\n"
        + "from emp e join bonus b\n"
        + "on e.ename = b.ename and e.deptno = 10";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  /** As {@link #testPushProjectWithOverPastJoin2()};
   * should not push over past join but should push the operands of over past
   * join. */
  @Test void testPushProjectWithOverPastJoin3() {
    final String sql = "select e.sal + b.comm,\n"
        + "sum(b.sal + b.sal + 100) over (partition by b.job)\n"
        + "from emp e join bonus b\n"
        + "on e.ename = b.ename and e.deptno = 10";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testPushProjectPastSetOp() {
    final String sql = "select sal from\n"
        + "(select * from emp e1 union all select * from emp e2)";
    sql(sql).withRule(ProjectSetOpTransposeRule.INSTANCE).check();
  }

  @Test void testPushJoinThroughUnionOnLeft() {
    final String sql = "select r1.sal from\n"
        + "(select * from emp e1 union all select * from emp e2) r1,\n"
        + "emp r2";
    sql(sql).withRule(JoinUnionTransposeRule.LEFT_UNION).check();
  }

  @Test void testPushJoinThroughUnionOnRight() {
    final String sql = "select r1.sal from\n"
        + "emp r1,\n"
        + "(select * from emp e1 union all select * from emp e2) r2";
    sql(sql).withRule(JoinUnionTransposeRule.RIGHT_UNION).check();
  }

  @Test void testPushJoinThroughUnionOnRightDoesNotMatchSemiJoin() {
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());

    // build a rel equivalent to sql:
    // select r1.sal from
    // emp r1 where r1.deptno in
    //  (select deptno from dept d1 where deptno > 100
    //  union all
    //  select deptno from dept d2 where deptno > 20)
    RelNode left = builder.scan("EMP").build();
    RelNode right = builder
        .scan("DEPT")
        .filter(
            builder.call(SqlStdOperatorTable.GREATER_THAN,
                builder.field("DEPTNO"),
                builder.literal(100)))
        .project(builder.field("DEPTNO"))
        .scan("DEPT")
        .filter(
            builder.call(SqlStdOperatorTable.GREATER_THAN,
                builder.field("DEPTNO"),
                builder.literal(20)))
        .project(builder.field("DEPTNO"))
        .union(true)
        .build();
    RelNode relNode = builder.push(left).push(right)
        .semiJoin(
            builder.call(SqlStdOperatorTable.EQUALS,
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")))
        .project(builder.field("SAL"))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(JoinUnionTransposeRule.RIGHT_UNION)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testPushJoinThroughUnionOnRightDoesNotMatchAntiJoin() {
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());

    // build a rel equivalent to sql:
    // select r1.sal from
    // emp r1 where r1.deptno not in
    //  (select deptno from dept d1 where deptno < 10
    //  union all
    //  select deptno from dept d2 where deptno > 20)
    RelNode left = builder.scan("EMP").build();
    RelNode right = builder
        .scan("DEPT")
        .filter(
            builder.call(SqlStdOperatorTable.LESS_THAN,
                builder.field("DEPTNO"),
                builder.literal(10)))
        .project(builder.field("DEPTNO"))
        .scan("DEPT")
        .filter(
            builder.call(SqlStdOperatorTable.GREATER_THAN,
                builder.field("DEPTNO"),
                builder.literal(20)))
        .project(builder.field("DEPTNO"))
        .union(true)
        .build();
    RelNode relNode = builder.push(left).push(right)
        .antiJoin(
            builder.call(SqlStdOperatorTable.EQUALS,
                builder.field(2, 0, "DEPTNO"),
                builder.field(2, 1, "DEPTNO")))
        .project(builder.field("SAL"))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(JoinUnionTransposeRule.RIGHT_UNION)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Test void testMergeFilterWithJoinCondition() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(JoinExtractFilterRule.INSTANCE)
        .addRuleInstance(FilterToCalcRule.INSTANCE)
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .addRuleInstance(CalcMergeRule.INSTANCE)
        .build();

    final String sql = "select d.name as dname,e.ename as ename\n"
        + " from emp e inner join dept d\n"
        + " on e.deptno=d.deptno\n"
        + " where d.name='Propane'";
    sql(sql).with(program).check();
  }

  /** Tests that filters are combined if they are identical. */
  @Test void testMergeFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterMergeRule.INSTANCE)
        .build();

    final String sql = "select name from (\n"
        + "  select *\n"
        + "  from dept\n"
        + "  where deptno = 10)\n"
        + "where deptno = 10\n";
    sql(sql).with(program).check();
  }

  /** Tests to see if the final branch of union is missed */
  @Test void testUnionMergeRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(ProjectRemoveRule.INSTANCE)
            .addRuleInstance(UnionMergeRule.INSTANCE)
            .build();

    final String sql = "select * from (\n"
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
        + ") aa\n";
    sql(sql).with(program).check();
  }

  @Test void testMinusMergeRule() throws Exception {
    HepProgram program = new HepProgramBuilder()
            .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
            .addRuleInstance(ProjectRemoveRule.INSTANCE)
            .addRuleInstance(UnionMergeRule.MINUS_INSTANCE)
            .build();

    final String sql = "select * from (\n"
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
        + ") aa\n";
    sql(sql).with(program).check();
  }

  /** Tests that a filters is combined are combined if they are identical,
   * even if one of them originates in an ON clause of a JOIN. */
  @Test void testMergeJoinFilter() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterMergeRule.INSTANCE)
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .build();

    final String sql = "select * from (\n"
        + "  select d.deptno, e.ename\n"
        + "  from emp as e\n"
        + "  join dept as d\n"
        + "  on e.deptno = d.deptno\n"
        + "  and d.deptno = 10)\n"
        + "where deptno = 10\n";
    sql(sql).with(program).check();
  }

  /** Tests {@link UnionMergeRule}, which merges 2 {@link Union} operators into
   * a single {@code Union} with 3 inputs. */
  @Test void testMergeUnionAll() throws Exception {
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
  @Test void testMergeUnionDistinct() throws Exception {
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
  @Test void testMergeUnionMixed() throws Exception {
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
  @Test void testMergeUnionMixed2() throws Exception {
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
  @Test void testMergeSetOpMixed() throws Exception {
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
  @Test void testMergeIntersect() throws Exception {
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
  @Test void testIntersectToDistinct() throws Exception {
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
  @Test void testIntersectToDistinctAll() throws Exception {
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
  @Test void testMergeMinus() throws Exception {
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
  @Test void testMergeMinusRightDeep() throws Exception {
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

  @Test void testHeterogeneousConversion() throws Exception {
    // This one tests the planner's ability to correctly
    // apply different converters on top of a common
    // sub-expression.  The common sub-expression is the
    // reference to the table sales.emps.  On top of that
    // are two projections, unioned at the top.  For one
    // of the projections, transfer it to calc, for the other,
    // keep it unchanged.
    HepProgram program = new HepProgramBuilder()
        // Control the calc conversion.
        .addMatchLimit(1)
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .build();

    final String sql = "select upper(ename) from emp union all\n"
        + "select lower(ename) from emp";
    sql(sql).with(program).check();
  }

  @Test void testPushSemiJoinPastJoinRuleLeft() throws Exception {
    // tests the case where the semijoin is pushed to the left
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and e1.empno = e2.empno";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinAddRedundantSemiJoinRule.INSTANCE,
        SemiJoinJoinTransposeRule.INSTANCE)
        .check();
  }

  @Test void testPushSemiJoinPastJoinRuleRight() throws Exception {
    // tests the case where the semijoin is pushed to the right
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinAddRedundantSemiJoinRule.INSTANCE,
        SemiJoinJoinTransposeRule.INSTANCE)
        .check();
  }

  @Test void testPushSemiJoinPastFilter() throws Exception {
    final String sql = "select e.ename from emp e, dept d\n"
        + "where e.deptno = d.deptno and e.ename = 'foo'";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinAddRedundantSemiJoinRule.INSTANCE,
        SemiJoinFilterTransposeRule.INSTANCE)
        .check();
  }

  @Test void testConvertMultiJoinRule() throws Exception {
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
        .build();
    sql(sql).with(program).check();
  }

  @Test void testManyFiltersOnTopOfMultiJoinShouldCollapse() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
        .addRuleCollection(
        Arrays.asList(FilterMultiJoinMergeRule.INSTANCE, ProjectMultiJoinMergeRule.INSTANCE))
        .build();
    final String sql = "select * from (select * from emp e1 left outer join dept d\n"
        + "on e1.deptno = d.deptno\n"
        + "where d.deptno > 3) where ename LIKE 'bar'";
    sql(sql).with(program).check();
  }

  @Test void testReduceConstants() throws Exception {
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
  @Test void testReduceConstantsDup() throws Exception {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where d.deptno=7 and d.deptno=8";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-935">[CALCITE-935]
   * Improve how ReduceExpressionsRule handles duplicate constraints</a>. */
  @Test void testReduceConstantsDup2() throws Exception {
    final String sql = "select *\n"
        + "from emp\n"
        + "where deptno=7 and deptno=8\n"
        + "and empno = 10 and mgr is null and empno = 10";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDup3() throws Exception {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where d.deptno<>7 or d.deptno<>8";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDup3Null() throws Exception {
    final String sql = "select e.empno"
        + " from emp e"
        + " where e.mgr<>7 or e.mgr<>8";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot() throws Exception {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where not(d.deptno=7 and d.deptno=8)";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNotNull() throws Exception {
    final String sql = "select e.empno"
        + " from emp e"
        + " where not(e.mgr=7 and e.mgr=8)";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot2() throws Exception {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where not(d.deptno=7 and d.name='foo' and d.deptno=8)";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot2Null() throws Exception {
    final String sql = "select e.empno"
        + " from emp e"
        + " where not(e.mgr=7 and e.deptno=8 and e.mgr=8)";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  @Test void testPullNull() throws Exception {
    final String sql = "select *\n"
        + "from emp\n"
        + "where deptno=7\n"
        + "and empno = 10 and mgr is null and empno = 10";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  @Test void testOrAlwaysTrue() {
    final String sql = "select * from EMPNULLABLES_20\n"
        + "where sal is null or sal is not null";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  @Test void testOrAlwaysTrue2() {
    final String sql = "select * from EMPNULLABLES_20\n"
        + "where sal is not null or sal is null";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  @Test void testReduceConstants2() throws Exception {
    final String sql = "select p1 is not distinct from p0\n"
        + "from (values (2, cast(null as integer))) as t(p0, p1)";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .checkUnchanged();
  }

  @Test void testReduceConstants3() throws Exception {
    final String sql = "select e.mgr is not distinct from f.mgr "
        + "from emp e join emp f on (e.mgr=f.mgr) where e.mgr is null";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-902">[CALCITE-902]
   * Match nullability when reducing expressions in a Project</a>. */
  @Test void testReduceConstantsProjectNullable() throws Exception {
    final String sql = "select mgr from emp where mgr=10";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  // see HIVE-9645
  @Test void testReduceConstantsNullEqualsOne() throws Exception {
    final String sql = "select count(1) from emp where cast(null as integer) = 1";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  // see HIVE-9644
  @Test void testReduceConstantsCaseEquals() throws Exception {
    final String sql = "select count(1) from emp\n"
        + "where case deptno\n"
        + "  when 20 then 2\n"
        + "  when 10 then 1\n"
        + "  else 3 end = 1";
    // Equivalent to 'deptno = 10'
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  @Test void testReduceConstantsCaseEquals2() throws Exception {
    final String sql = "select count(1) from emp\n"
        + "where case deptno\n"
        + "  when 20 then 2\n"
        + "  when 10 then 1\n"
        + "  else cast(null as integer) end = 1";

    // Equivalent to 'case when deptno = 20 then false
    //                     when deptno = 10 then true
    //                     else null end'
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  @Test void testReduceConstantsCaseEquals3() throws Exception {
    final String sql = "select count(1) from emp\n"
        + "where case deptno\n"
        + "  when 30 then 1\n"
        + "  when 20 then 2\n"
        + "  when 10 then 1\n"
        + "  when 30 then 111\n"
        + "  else 0 end = 1";

    // Equivalent to 'deptno = 30 or deptno = 10'
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  @Test void testSkipReduceConstantsCaseEquals() throws Exception {
    final String sql = "select * from emp e1, emp e2\n"
        + "where coalesce(e1.mgr, -1) = coalesce(e2.mgr, -1)";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN)
        .check();
  }

  @Test void testReduceConstantsEliminatesFilter() throws Exception {
    final String sql = "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)";

    // WHERE NULL is the same as WHERE FALSE, so get empty result
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1860">[CALCITE-1860]
   * Duplicate null predicates cause NullPointerException in RexUtil</a>. */
  @Test void testReduceConstantsNull() throws Exception {
    final String sql = "select * from (\n"
        + "  select *\n"
        + "  from (\n"
        + "    select cast(null as integer) as n\n"
        + "    from emp)\n"
        + "  where n is null and n is null)\n"
        + "where n is null";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-566">[CALCITE-566]
   * ReduceExpressionsRule requires planner to have an Executor</a>. */
  @Test void testReduceConstantsRequiresExecutor() throws Exception {
    // Remove the executor
    tester.convertSqlToRel("values 1").rel.getCluster().getPlanner()
        .setExecutor(null);

    // Rule should not fire, but there should be no NPE
    final String sql =
        "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  @Test void testAlreadyFalseEliminatesFilter() throws Exception {
    final String sql = "select * from (values (1,2)) where false";

    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  @Test void testReduceConstantsCalc() throws Exception {
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
    final String sql = "select * from (\n"
        + "  select upper(substring(x FROM 1 FOR 2) || substring(x FROM 3)) as u,\n"
        + "      substring(x FROM 1 FOR 1) as s\n"
        + "  from (\n"
        + "    select 'table' as x from (values (true))\n"
        + "    union\n"
        + "    select 'view' from (values (true))\n"
        + "    union\n"
        + "    select 'foreign table' from (values (true))\n"
        + "  )\n"
        + ") where u = 'TABLE'";
    sql(sql).with(program).check();
  }

  @Test void testRemoveSemiJoin() throws Exception {
    final String sql = "select e.ename from emp e, dept d\n"
        + "where e.deptno = d.deptno";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinAddRedundantSemiJoinRule.INSTANCE,
        SemiJoinRemoveRule.INSTANCE)
        .check();
  }

  @Test void testRemoveSemiJoinWithFilter() throws Exception {
    final String sql = "select e.ename from emp e, dept d\n"
        + "where e.deptno = d.deptno and e.ename = 'foo'";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinAddRedundantSemiJoinRule.INSTANCE,
        SemiJoinFilterTransposeRule.INSTANCE,
        SemiJoinRemoveRule.INSTANCE)
        .check();
  }

  @Test void testRemoveSemiJoinRight() throws Exception {
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinAddRedundantSemiJoinRule.INSTANCE,
        SemiJoinJoinTransposeRule.INSTANCE,
        SemiJoinRemoveRule.INSTANCE)
        .check();
  }

  @Test void testRemoveSemiJoinRightWithFilter() throws Exception {
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno\n"
        + "and d.name = 'foo'";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinAddRedundantSemiJoinRule.INSTANCE,
        SemiJoinJoinTransposeRule.INSTANCE,
        SemiJoinFilterTransposeRule.INSTANCE,
        SemiJoinRemoveRule.INSTANCE)
        .check();
  }

  private void checkPlanning(String query) throws Exception {
    final Tester tester1 = tester.withCatalogReaderFactory(
        (typeFactory, caseSensitive) -> new MockCatalogReader(typeFactory, caseSensitive) {
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
        });
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(ProjectRemoveRule.INSTANCE)
        .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
        .build();
    sql(query).withTester(t -> tester1)
        .with(program)
        .check();
  }

  @Test void testConvertMultiJoinRuleOuterJoins() throws Exception {
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

  @Test void testConvertMultiJoinRuleOuterJoins2() throws Exception {
    // in (A right join B) join C, pushing C is not allowed;
    // therefore there should be 2 MultiJoin
    checkPlanning("select * from A right join B on a = b join C on b = c");
  }

  @Test void testConvertMultiJoinRuleOuterJoins3() throws Exception {
    // in (A join B) left join C, pushing C is allowed;
    // therefore there should be 1 MultiJoin
    checkPlanning("select * from A join B on a = b left join C on b = c");
  }

  @Test void testConvertMultiJoinRuleOuterJoins4() throws Exception {
    // in (A join B) right join C, pushing C is not allowed;
    // therefore there should be 2 MultiJoin
    checkPlanning("select * from A join B on a = b right join C on b = c");
  }

  @Test void testPushSemiJoinPastProject() throws Exception {
    final String sql = "select e.* from\n"
        + "(select ename, trim(job), sal * 2, deptno from emp) e, dept d\n"
        + "where e.deptno = d.deptno";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinAddRedundantSemiJoinRule.INSTANCE,
        SemiJoinProjectTransposeRule.INSTANCE)
        .check();
  }

  @Test void testReduceValuesUnderFilter() throws Exception {
    // Plan should be same as for
    // select a, b from (values (10,'x')) as t(a, b)");
    final String sql = "select a, b from (values (10, 'x'), (20, 'y')) as t(a, b) where a < 15";
    sql(sql).withRule(FilterProjectTransposeRule.INSTANCE,
        ValuesReduceRule.FILTER_INSTANCE)
        .check();
  }

  @Test void testReduceValuesUnderProject() throws Exception {
    // Plan should be same as for
    // select a, b as x from (values (11), (23)) as t(x)");
    final String sql = "select a + b from (values (10, 1), (20, 3)) as t(a, b)";
    sql(sql).withRule(ProjectMergeRule.INSTANCE,
        ValuesReduceRule.PROJECT_INSTANCE)
        .check();
  }

  @Test void testReduceValuesUnderProjectFilter() throws Exception {
    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    final String sql = "select a + b as x, b, a\n"
        + "from (values (10, 1), (30, 7), (20, 3)) as t(a, b)\n"
        + "where a - b < 21";
    sql(sql).withRule(FilterProjectTransposeRule.INSTANCE,
        ProjectMergeRule.INSTANCE,
        ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1439">[CALCITE-1439]
   * Handling errors during constant reduction</a>. */
  @Test void testReduceCase() throws Exception {
    final String sql = "select\n"
        + "  case when false then cast(2.1 as float)\n"
        + "   else cast(1 as integer) end as newcol\n"
        + "from emp";
    sql(sql).withRule(ReduceExpressionsRule.PROJECT_INSTANCE)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  private void checkReduceNullableToNotNull(ReduceExpressionsRule rule) {
    final String sql = "select\n"
        + "  empno + case when 'a' = 'a' then 1 else null end as newcol\n"
        + "from emp";
    sql(sql).withRule(rule)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  /** Test case that reduces a nullable expression to a NOT NULL literal that
   *  is cast to nullable. */
  @Test void testReduceNullableToNotNull() throws Exception {
    checkReduceNullableToNotNull(ReduceExpressionsRule.PROJECT_INSTANCE);
  }

  /** Test case that reduces a nullable expression to a NOT NULL literal. */
  @Test void testReduceNullableToNotNull2() throws Exception {
    final ReduceExpressionsRule.ProjectReduceExpressionsRule rule =
        new ReduceExpressionsRule.ProjectReduceExpressionsRule(
            LogicalProject.class, false,
            RelFactories.LOGICAL_BUILDER);
    checkReduceNullableToNotNull(rule);
  }

  @Test void testReduceConstantsIsNull() throws Exception {
    final String sql = "select empno from emp where empno=10 and empno is null";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  @Test void testReduceConstantsIsNotNull() throws Exception {
    final String sql = "select empno from emp\n"
        + "where empno=10 and empno is not null";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  @Test void testReduceConstantsNegated() throws Exception {
    final String sql = "select empno from emp\n"
        + "where empno=10 and not(empno=10)";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  @Test void testReduceConstantsNegatedInverted() throws Exception {
    final String sql = "select empno from emp where empno>10 and empno<=10";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2638">[CALCITE-2638]
   * Constant reducer must not duplicate calls to non-deterministic
   * functions</a>. */
  @Test void testReduceConstantsNonDeterministicFunction() {
    final DiffRepository diffRepos = getDiffRepos();

    final SqlOperator nonDeterministicOp =
        new SqlSpecialOperator("NDC", SqlKind.OTHER_FUNCTION, 0, false,
            ReturnTypes.INTEGER, null, null) {
          @Override public boolean isDeterministic() {
            return false;
          }
        };

    // Build a tree equivalent to the SQL
    //  SELECT sal, n
    //  FROM (SELECT sal, NDC() AS n FROM emp)
    //  WHERE n > 10
    final RelBuilder builder =
        RelBuilder.create(RelBuilderTest.config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("SAL"),
                builder.alias(builder.call(nonDeterministicOp), "N"))
            .filter(
                builder.call(SqlStdOperatorTable.GREATER_THAN,
                    builder.field("N"), builder.literal(10)))
            .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  /** Checks that constant reducer duplicates calls to dynamic functions, if
   * appropriate. CURRENT_TIMESTAMP is a dynamic function. */
  @Test void testReduceConstantsDynamicFunction() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .build();

    final String sql = "select sal, t\n"
        + "from (select sal, current_timestamp t from emp)\n"
        + "where t > TIMESTAMP '2018-01-01 00:00:00'";
    sql(sql).with(program).checkUnchanged();
  }

  @Test void testCasePushIsAlwaysWorking() throws Exception {
    final String sql = "select empno from emp"
        + " where case when sal > 1000 then empno else sal end = 1";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.CALC_INSTANCE,
        ReduceExpressionsRule.PROJECT_INSTANCE)
        .check();
  }

  @Test void testReduceValuesNull() throws Exception {
    // The NULL literal presents pitfalls for value-reduction. Only
    // an INSERT statement contains un-CASTed NULL values.
    final String sql = "insert into EMPNULLABLES(EMPNO, ENAME, JOB) (select 0, 'null', NULL)";
    sql(sql).withRule(ValuesReduceRule.PROJECT_INSTANCE).check();
  }

  @Test void testReduceValuesToEmpty() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
        .build();

    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    final String sql = "select a + b as x, b, a from (values (10, 1), (30, 7)) as t(a, b)\n"
        + "where a - b < 0";
    sql(sql).with(program).check();
  }

  @Test void testReduceConstantsWindow() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectToWindowRule.PROJECT)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectWindowTransposeRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.WINDOW_INSTANCE)
        .build();

    final String sql = "select col1, col2, col3\n"
        + "from (\n"
        + "  select empno,\n"
        + "    sum(100) over (partition by deptno, sal order by sal) as col1,\n"
        + "    sum(100) over (partition by sal order by deptno) as col2,\n"
        + "    sum(sal) over (partition by deptno order by sal) as col3\n"
        + "  from emp where sal = 5000)";

    sql(sql).with(program).check();
  }

  @Test void testEmptyFilterProjectUnion() throws Exception {
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
    final String sql = "select * from (\n"
        + "select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "union all\n"
        + "select * from (values (20, 2))\n"
        + ")\n"
        + "where x + y > 30";
    sql(sql).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1488">[CALCITE-1488]
   * ValuesReduceRule should ignore empty Values</a>. */
  @Test void testEmptyProject() throws Exception {
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
  @Test void testEmptyProject2() throws Exception {
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

  @Test void testEmptyIntersect() throws Exception {
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

  @Test void testEmptyMinus() throws Exception {
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

  @Test void testEmptyMinus2() throws Exception {
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

  @Test void testLeftEmptyInnerJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    final String sql = "select * from (\n"
        + "select * from emp where false) as e\n"
        + "join dept as d on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testLeftEmptyLeftJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    final String sql = "select * from (\n"
        + "  select * from emp where false) e\n"
        + "left join dept d on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testLeftEmptyRightJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be equivalent to "select * from emp right join dept".
    // Cannot optimize away the join because of RIGHT.
    final String sql = "select * from (\n"
        + "  select * from emp where false) e\n"
        + "right join dept d on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testLeftEmptyFullJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be equivalent to "select * from emp full join dept".
    // Cannot optimize away the join because of FULL.
    final String sql = "select * from (\n"
        + "  select * from emp where false) e\n"
        + "full join dept d on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testLeftEmptySemiJoin() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode relNode = relBuilder
        .scan("EMP").empty()
        .scan("DEPT")
        .semiJoin(relBuilder
            .equals(
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")))
        .project(relBuilder.field("EMPNO"))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    final RelNode output = hepPlanner.findBestExp();

    final String planBefore = NL + RelOptUtil.toString(relNode);
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    // Plan should be empty
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testLeftEmptyAntiJoin() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode relNode = relBuilder
        .scan("EMP").empty()
        .scan("DEPT")
        .antiJoin(relBuilder
            .equals(
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")))
        .project(relBuilder.field("EMPNO"))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    final RelNode output = hepPlanner.findBestExp();

    final String planBefore = NL + RelOptUtil.toString(relNode);
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    // Plan should be empty
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testRightEmptyInnerJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    final String sql = "select * from emp e\n"
        + "join (select * from dept where false) as d\n"
        + "on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testRightEmptyLeftJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be equivalent to "select * from emp left join dept".
    // Cannot optimize away the join because of LEFT.
    final String sql = "select * from emp e\n"
        + "left join (select * from dept where false) as d\n"
        + "on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testRightEmptyRightJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be empty
    final String sql = "select * from emp e\n"
        + "right join (select * from dept where false) as d\n"
        + "on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testRightEmptyFullJoin() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    // Plan should be equivalent to "select * from emp full join dept".
    // Cannot optimize away the join because of FULL.
    final String sql = "select * from emp e\n"
        + "full join (select * from dept where false) as d\n"
        + "on e.deptno = d.deptno";
    sql(sql).with(program).check();
  }

  @Test void testRightEmptySemiJoin() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode relNode = relBuilder
        .scan("EMP")
        .scan("DEPT").empty()
        .semiJoin(relBuilder
            .equals(
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")))
        .project(relBuilder.field("EMPNO"))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    final RelNode output = hepPlanner.findBestExp();

    final String planBefore = NL + RelOptUtil.toString(relNode);
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    // Plan should be empty
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testRightEmptyAntiJoin() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode relNode = relBuilder
        .scan("EMP")
        .scan("DEPT").empty()
        .antiJoin(relBuilder
            .equals(
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")))
        .project(relBuilder.field("EMPNO"))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    final RelNode output = hepPlanner.findBestExp();

    final String planBefore = NL + RelOptUtil.toString(relNode);
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    // Plan should be scan("EMP") (i.e. join's left child)
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testRightEmptyAntiJoinNonEqui() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode relNode = relBuilder
        .scan("EMP")
        .scan("DEPT").empty()
        .antiJoin(relBuilder
            .equals(
                relBuilder.field(2, 0, "DEPTNO"),
                relBuilder.field(2, 1, "DEPTNO")),
            relBuilder
                .equals(
                    relBuilder.field(2, 0, "SAL"),
                    relBuilder.literal(2000)))
        .project(relBuilder.field("EMPNO"))
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .build();

    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    final RelNode output = hepPlanner.findBestExp();

    final String planBefore = NL + RelOptUtil.toString(relNode);
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    // Plan should be scan("EMP") (i.e. join's left child)
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testEmptySort() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .addRuleInstance(PruneEmptyRules.SORT_INSTANCE)
        .build();

    final String sql = "select * from emp where false order by deptno";
    sql(sql).with(program).check();
  }

  @Test void testEmptySort2() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode relNode = relBuilder
        .scan("DEPT").empty()
        .sort(
            relBuilder.field("DNAME"),
            relBuilder.field("DEPTNO"))
        .build();

    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PruneEmptyRules.SORT_INSTANCE)
        .build();

    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    final RelNode output = hepPlanner.findBestExp();

    final String planBefore = NL + RelOptUtil.toString(relNode);
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testEmptySortLimitZero() {
    final String sql = "select * from emp order by deptno limit 0";
    sql(sql).withRule(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE).check();
  }

  @Test void testEmptyAggregate() {
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
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test void testEmptyAggregateEmptyKey() {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE)
        .build();
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(PruneEmptyRules.AGGREGATE_INSTANCE)
        .build();

    final String sql = "select sum(empno) from emp where false";
    sql(sql).withPre(preProgram)
        .with(program)
        .checkUnchanged();
  }

  @Test void testEmptyAggregateEmptyKeyWithAggregateValuesRule() {
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

  @Test void testReduceCasts() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    // The resulting plan should have no cast expressions
    final String sql = "select cast(d.name as varchar(128)), cast(e.empno as integer)\n"
        + "from dept as d inner join emp as e\n"
        + "on cast(d.deptno as integer) = cast(e.deptno as integer)\n"
        + "where cast(e.job as varchar(1)) = 'Manager'";
    sql(sql).with(program)
        .checkUnchanged();
  }

  /** Tests that a cast from a TIME to a TIMESTAMP is not reduced. It is not
   * constant because the result depends upon the current date. */
  @Test void testReduceCastTimeUnchanged() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
        .build();

    sql("select cast(time '12:34:56' as timestamp) from emp as e")
        .with(program)
        .checkUnchanged();
  }

  @Test void testReduceCastAndConsts() throws Exception {
    // Make sure constant expressions inside the cast can be reduced
    // in addition to the casts.
    final String sql = "select * from emp\n"
        + "where cast((empno + (10/2)) as int) = 13";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE).check();
  }

  @Test public <T> void testReduceCaseNullabilityChange() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
        .build();

    final String sql = "select case when empno = 1 then 1\n"
        + "when 1 IS NOT NULL then 2\n"
        + "else null end as qx "
        + "from emp";
    sql(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .with(program).check();
  }

  @Test void testReduceCastsNullable() throws Exception {
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
    final String sql = "insert into sales.dept(deptno, name)\n"
        + "select empno, cast(job as varchar(128)) from sales.empnullables";
    sql(sql).with(program).check();
  }

  @Test void testReduceCaseWhenWithCast() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    final RelDataType type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    RelNode left = relBuilder
        .values(new String[]{"x", "y"}, 1, 2).build();
    RexNode ref = rexBuilder.makeInputRef(left, 0);
    RexNode literal1 = rexBuilder.makeLiteral(1, type, false);
    RexNode literal2 = rexBuilder.makeLiteral(2, type, false);
    RexNode literal3 = rexBuilder.makeLiteral(3, type, false);

    // CASE WHEN x % 2 = 1 THEN x < 2
    //      WHEN x % 3 = 2 THEN x < 1
    //      ELSE x < 3
    final RexNode caseRexNode = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeCall(SqlStdOperatorTable.MOD, ref, literal2), literal1),
        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal2),
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeCall(SqlStdOperatorTable.MOD, ref, literal3), literal2),
        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal1),
        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal3));

    final RexNode castNode = rexBuilder.makeCast(rexBuilder.getTypeFactory().
        createTypeWithNullability(caseRexNode.getType(), true), caseRexNode);
    final RelNode root = relBuilder
        .push(left)
        .project(castNode)
        .build();

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);

    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
    hepPlanner.setRoot(root);

    RelNode output = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  private void basePushAggThroughUnion() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectSetOpTransposeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateUnionTransposeRule.INSTANCE)
        .build();
    sql("${sql}").with(program).check();
  }

  @Test void testPushSumConstantThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullConstantThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableNOGBYThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountStarThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushMaxNullableThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushMinThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushAvgThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumCountStarThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumConstantGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullConstantGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountStarGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushMaxNullableGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushMinGroupingSetsThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushAvgGroupingSetsThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushSumCountStarGroupingSetsThroughUnion() throws
      Exception {
    basePushAggThroughUnion();
  }

  @Test void testPushCountFilterThroughUnion() throws Exception {
    basePushAggThroughUnion();
  }

  @Test void testPullFilterThroughAggregate() throws Exception {
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
    sql(sql).withPre(preProgram).with(program).check();
  }

  @Test void testPullFilterThroughAggregateGroupingSets()
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
    sql(sql).withPre(preProgram).with(program).check();
  }

  private void basePullConstantTroughAggregate() throws Exception {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateProjectPullUpConstantsRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    sql("${sql}").with(program).check();
  }

  @Test void testPullConstantThroughConstLast() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateSimpleNonNullable() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregatePermuted() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregatePermutedConstFirst() throws
      Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregatePermutedConstGroupBy()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateConstGroupBy()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateAllConst()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateAllLiterals()
      throws Exception {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughUnion()
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

  @Test void testPullConstantThroughUnion2()
      throws Exception {
    // Negative test: constants should not be pulled up
    HepProgram program = HepProgram.builder()
        .addRuleInstance(UnionPullUpConstantsRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select 2, deptno, job from emp as e1\n"
        + "union all\n"
        + "select 1, deptno, job from emp as e2";
    sql(sql).with(program).checkUnchanged();
  }

  @Test void testPullConstantThroughUnion3()
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

  @Test void testAggregateProjectMerge() {
    final String sql = "select x, sum(z), y from (\n"
        + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
        + "  from emp)\n"
        + "group by x, y";
    sql(sql).withRule(AggregateProjectMergeRule.INSTANCE).check();
  }

  @Test void testAggregateGroupingSetsProjectMerge() {
    final String sql = "select x, sum(z), y from (\n"
        + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
        + "  from emp)\n"
        + "group by rollup(x, y)";
    sql(sql).withRule(AggregateProjectMergeRule.INSTANCE).check();
  }

  @Test void testAggregateExtractProjectRule() {
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

  @Test void testAggregateExtractProjectRuleWithGroupingSets() {
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
  @Test void testAggregateExtractProjectRuleWithGroupingSets2() {
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

  @Test void testAggregateExtractProjectRuleWithFilter() {
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
                operandJ(Project.class, null,
                    new Predicate<Project>() {
                      int matchCount = 0;

                      public boolean test(Project project) {
                        return matchCount++ == 0;
                      }
                    },
                    none())),
            RelFactories.LOGICAL_BUILDER);
    sql(sql).withPre(pre).withRule(rule).checkUnchanged();
  }

  @Test void testAggregateCaseToFilter() {
    final String sql = "select\n"
        + " sum(sal) as sum_sal,\n"
        + " count(distinct case\n"
        + "       when job = 'CLERK'\n"
        + "       then deptno else null end) as count_distinct_clerk,\n"
        + " sum(case when deptno = 10 then sal end) as sum_sal_d10,\n"
        + " sum(case when deptno = 20 then sal else 0 end) as sum_sal_d20,\n"
        + " sum(case when deptno = 30 then 1 else 0 end) as count_d30,\n"
        + " count(case when deptno = 40 then 'x' end) as count_d40,\n"
        + " count(case when deptno = 20 then 1 end) as count_d20\n"
        + "from emp";
    sql(sql).withRule(AggregateCaseToFilterRule.INSTANCE).check();
  }

  @Test void testPullAggregateThroughUnion() {
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

  @Test void testPullAggregateThroughUnion2() {
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

  /**
   * Once the bottom aggregate pulled through union, we need to add a Project
   * if the new input contains a different type from the union.
   */
  @Test void testPullAggregateThroughUnionAndAddProjects() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateUnionAggregateRule.INSTANCE)
        .build();

    final String sql = "select job, deptno from"
        + " (select job, deptno from emp as e1"
        + " group by job, deptno"
        + "  union all"
        + " select job, deptno from emp as e2"
        + " group by job, deptno)"
        + " group by job, deptno";
    sql(sql).with(program).check();
  }

  /**
   * Make sure the union alias is preserved when the bottom aggregate is
   * pulled up through union.
   */
  @Test void testPullAggregateThroughUnionWithAlias() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateUnionAggregateRule.INSTANCE)
        .build();

    final String sql = "select job, c from"
        + " (select job, deptno c from emp as e1"
        + " group by job, deptno"
        + "  union all"
        + " select job, deptno from emp as e2"
        + " group by job, deptno)"
        + " group by job, c";
    sql(sql).with(program).check();
  }

  /**
   * Create a {@link HepProgram} with common transitive rules.
   */
  private HepProgram getTransitiveProgram() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.DUMB_FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
        .addRuleInstance(FilterSetOpTransposeRule.INSTANCE)
        .build();
    return program;
  }

  @Test void testTransitiveInferenceJoin() throws Exception {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno where e.deptno > 7";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceProject() throws Exception {
    final String sql = "select 1 from (select * from sales.emp where deptno > 7) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceAggregate() throws Exception {
    final String sql = "select 1 from (select deptno, count(*) from sales.emp where deptno > 7\n"
        + "group by deptno) d inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceUnion() throws Exception {
    final String sql = "select 1 from\n"
        + "(select deptno from sales.emp where deptno > 7\n"
        + "union all select deptno from sales.emp where deptno > 10) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceJoin3way() throws Exception {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "inner join sales.emp f on e.deptno = f.deptno\n"
        + "where d.deptno > 7";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceJoin3wayAgg() throws Exception {
    final String sql = "select 1 from\n"
        + "(select deptno, count(*) from sales.emp where deptno > 7 group by deptno) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "inner join sales.emp f on e.deptno = f.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceLeftOuterJoin() throws Exception {
    final String sql = "select 1 from sales.emp d\n"
        + "left outer join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno > 7 and e.deptno > 9";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceRightOuterJoin() throws Exception {
    final String sql = "select 1 from sales.emp d\n"
        + "right outer join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno > 7 and e.deptno > 9";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceFullOuterJoin() throws Exception {
    final String sql = "select 1 from sales.emp d full outer join sales.emp e\n"
        + "on d.deptno = e.deptno  where d.deptno > 7 and e.deptno > 9";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).checkUnchanged();
  }

  @Test void testTransitiveInferencePreventProjectPullUp()
      throws Exception {
    final String sql = "select 1 from (select comm as deptno from sales.emp where deptno > 7) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).checkUnchanged();
  }

  @Test void testTransitiveInferencePullUpThruAlias() throws Exception {
    final String sql = "select 1 from (select comm as deptno from sales.emp where comm > 7) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceConjunctInPullUp() throws Exception {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno in (7, 9) or d.deptno > 10";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceNoPullUpExprs() throws Exception {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno in (7, 9) or d.comm > 10";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).checkUnchanged();
  }

  @Test void testTransitiveInferenceUnion3way() throws Exception {
    final String sql = "select 1 from\n"
        + "(select deptno from sales.emp where deptno > 7\n"
        + "union all\n"
        + "select deptno from sales.emp where deptno > 10\n"
        + "union all\n"
        + "select deptno from sales.emp where deptno > 1) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceUnion3wayOr() throws Exception {
    final String sql = "select 1 from\n"
        + "(select empno, deptno from sales.emp where deptno > 7 or empno < 10\n"
        + "union all\n"
        + "select empno, deptno from sales.emp where deptno > 10 or empno < deptno\n"
        + "union all\n"
        + "select empno, deptno from sales.emp where deptno > 1) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-443">[CALCITE-443]
   * getPredicates from a union is not correct</a>. */
  @Test void testTransitiveInferenceUnionAlwaysTrue() throws Exception {
    final String sql = "select d.deptno, e.deptno from\n"
        + "(select deptno from sales.emp where deptno < 4) d\n"
        + "inner join\n"
        + "(select deptno from sales.emp where deptno > 7\n"
        + "union all select deptno from sales.emp) e\n"
        + "on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testTransitiveInferenceConstantEquiPredicate()
      throws Exception {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno  where 1 = 1";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).checkUnchanged();
  }

  @Test void testTransitiveInferenceComplexPredicate() throws Exception {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno > 7 and e.sal = e.deptno and d.comm = d.deptno\n"
        + "and d.comm + d.deptno > d.comm/2";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE).check();
  }

  @Test void testPullConstantIntoProject() throws Exception {
    final String sql = "select deptno, deptno + 1, empno + deptno\n"
        + "from sales.emp where deptno = 10";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE,
            ReduceExpressionsRule.PROJECT_INSTANCE)
        .check();
  }

  @Test void testPullConstantIntoFilter() throws Exception {
    final String sql = "select * from (select * from sales.emp where deptno = 10)\n"
        + "where deptno + 5 > empno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE,
            ReduceExpressionsRule.FILTER_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1995">[CALCITE-1995]
   * Remove predicates from Filter if they can be proved to be always true or
   * false</a>. */
  @Test void testSimplifyFilter() throws Exception {
    final String sql = "select * from (select * from sales.emp where deptno > 10)\n"
        + "where empno > 3 and deptno > 5";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE,
            ReduceExpressionsRule.FILTER_INSTANCE)
        .check();
  }

  @Test void testPullConstantIntoJoin() throws Exception {
    final String sql = "select * from (select * from sales.emp where empno = 10) as e\n"
        + "left join sales.dept as d on e.empno = d.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(JoinPushTransitivePredicatesRule.INSTANCE,
            ReduceExpressionsRule.JOIN_INSTANCE)
        .check();
  }

  @Test void testPullConstantIntoJoin2() throws Exception {
    final String sql = "select * from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.empno = d.deptno and e.deptno + e.empno = d.deptno + 5";
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(JoinPushTransitivePredicatesRule.INSTANCE)
        .addRuleCollection(
            ImmutableList.of(
                ReduceExpressionsRule.PROJECT_INSTANCE,
                FilterProjectTransposeRule.INSTANCE,
                ReduceExpressionsRule.JOIN_INSTANCE))
        .build();
    sql(sql).withPre(getTransitiveProgram()).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2110">[CALCITE-2110]
   * ArrayIndexOutOfBoundsException in RexSimplify when using
   * ReduceExpressionsRule.JOIN_INSTANCE</a>. */
  @Test void testCorrelationScalarAggAndFilter() {
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3111">[CALCITE-3111]
   * Allow custom implementations of Correlate in RelDecorrelator </a>
   */
  @Test void testCustomDecorrelate() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)";

    // Convert sql to rel
    RelRoot root = tester.convertSqlToRel(sql);

    // Create a duplicate rel tree with a custom correlate instead of logical correlate
    LogicalCorrelate logicalCorrelate = (LogicalCorrelate) root.rel.getInput(0).getInput(0);
    CustomCorrelate customCorrelate = new CustomCorrelate(
        logicalCorrelate.getCluster(),
        logicalCorrelate.getTraitSet(),
        logicalCorrelate.getLeft(),
        logicalCorrelate.getRight(),
        logicalCorrelate.getCorrelationId(),
        logicalCorrelate.getRequiredColumns(),
        logicalCorrelate.getJoinType());
    RelNode newRoot = root.rel.copy(
        root.rel.getTraitSet(),
        ImmutableList.of(
            root.rel.getInput(0).copy(
                root.rel.getInput(0).getTraitSet(),
                ImmutableList.<RelNode>of(customCorrelate))));

    // Decorrelate both trees using the same relBuilder
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode logicalDecorrelated = RelDecorrelator.decorrelateQuery(root.rel, relBuilder);
    RelNode customDecorrelated = RelDecorrelator.decorrelateQuery(newRoot, relBuilder);
    String logicalDecorrelatedPlan = NL + RelOptUtil.toString(logicalDecorrelated);
    String customDecorrelatedPlan = NL + RelOptUtil.toString(customDecorrelated);

    // Ensure that the plans are equal
    getDiffRepos().assertEquals("Comparing Plans from LogicalCorrelate and CustomCorrelate",
        logicalDecorrelatedPlan, customDecorrelatedPlan);
  }

  @Test void testProjectWindowTransposeRule() {
    final String sql = "select count(empno) over(), deptno from emp";
    sql(sql).withRule(ProjectToWindowRule.PROJECT,
        ProjectWindowTransposeRule.INSTANCE)
        .check();
  }

  @Test void testProjectWindowTransposeRuleWithConstants() {
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

    sql(sql).with(program).check();
  }

  /** While it's probably valid relational algebra for a Project to contain
   * a RexOver inside a RexOver, ProjectMergeRule should not bring it about. */
  @Test void testProjectMergeShouldIgnoreOver() {
    final String sql = "select row_number() over (order by deptno), col1\n"
        + "from (\n"
        + "  select deptno,\n"
        + "    sum(100) over (partition by  deptno order by sal) as col1\n"
        + "  from emp)";
    sql(sql).withRule(ProjectMergeRule.INSTANCE).checkUnchanged();
  }

  @Test void testAggregateProjectPullUpConstants() {
    final String sql = "select job, empno, sal, sum(sal) as s\n"
        + "from emp where empno = 10\n"
        + "group by job, empno, sal";
    sql(sql).withRule(AggregateProjectPullUpConstantsRule.INSTANCE2).check();
  }

  @Test void testAggregateProjectPullUpConstants2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select ename, sal\n"
        + "from (select '1', ename, sal from emp where ename = 'John') subq\n"
        + "group by ename, sal";
    sql(sql).withPre(preProgram)
        .withRule(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .check();
  }

  @Test void testPushFilterWithRank() throws Exception {
    final String sql = "select e1.ename, r\n"
        + "from (\n"
        + "  select ename, "
        + "  rank() over(partition by  deptno order by sal) as r "
        + "  from emp) e1\n"
        + "where r < 2";
    sql(sql).withRule(FilterProjectTransposeRule.INSTANCE)
        .checkUnchanged();
  }

  @Test void testPushFilterWithRankExpr() throws Exception {
    final String sql = "select e1.ename, r\n"
        + "from (\n"
        + "  select ename,\n"
        + "  rank() over(partition by  deptno order by sal) + 1 as r "
        + "  from emp) e1\n"
        + "where r < 2";
    sql(sql).withRule(FilterProjectTransposeRule.INSTANCE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-841">[CALCITE-841]
   * Redundant windows when window function arguments are expressions</a>. */
  @Test void testExpressionInWindowFunction() {
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
  @Test void testWindowInParenthesis() {
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

  /** Test case for DX-11490
   * Make sure the planner doesn't fail over wrong push down
   * of is null */
  @Test void testIsNullPushDown() {
    HepProgramBuilder preBuilder = new HepProgramBuilder();
    preBuilder.addRuleInstance(ProjectToWindowRule.PROJECT);

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);
    builder.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
    HepPlanner hepPlanner = new HepPlanner(builder.build());

    final String sql = "select empno, deptno, w_count from (\n"
        + "  select empno, deptno, count(empno) over (w) w_count\n"
        + "  from emp\n"
        + "  window w as (partition by deptno order by empno)\n"
        + ") sub_query where w_count is null";
    sql(sql)
        .withPre(preBuilder.build())
        .with(hepPlanner)
        .check();
  }

  @Test void testIsNullPushDown2() {
    HepProgramBuilder preBuilder = new HepProgramBuilder();
    preBuilder.addRuleInstance(ProjectToWindowRule.PROJECT);

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);
    builder.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
    HepPlanner hepPlanner = new HepPlanner(builder.build());

    final String sql = "select empno, deptno, w_count from (\n"
        + "  select empno, deptno, count(empno) over (ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) w_count\n"
        + "  from emp\n"
        + ") sub_query where w_count is null";
    sql(sql)
        .withPre(preBuilder.build())
        .with(hepPlanner)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-750">[CALCITE-750]
   * Allow windowed aggregate on top of regular aggregate</a>. */
  @Test void testNestedAggregates() {
    final String sql = "SELECT\n"
        + "  avg(sum(sal) + 2 * min(empno) + 3 * avg(empno))\n"
        + "  over (partition by deptno)\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).withRule(ProjectToWindowRule.PROJECT).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2078">[CALCITE-2078]
   * Aggregate functions in OVER clause</a>. */
  @Test void testWindowFunctionOnAggregations() {
    final String sql = "SELECT\n"
        + "  min(empno),\n"
        + "  sum(sal),\n"
        + "  sum(sum(sal))\n"
        + "    over (partition by min(empno) order by sum(sal))\n"
        + "from emp\n"
        + "group by deptno";
    sql(sql).withRule(ProjectToWindowRule.PROJECT).check();
  }

  @Test void testPushAggregateThroughJoin1() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job,d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by on non-join keys, group by on non-null generating side only */
  @Test void testPushAggregateThroughOuterJoin1() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.ename\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "group by e.ename";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by on non-join keys, on null generating side only */
  @Test void testPushAggregateThroughOuterJoin2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select d.ename\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.emp as d on e.job = d.job\n"
        + "group by d.ename";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by on both side on non-join keys */
  @Test void testPushAggregateThroughOuterJoin3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.ename, d.mgr\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.emp as d on e.job = d.job\n"
        + "group by e.ename,d.mgr";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by on key same as join key, group by on non-null generating side */
  @Test void testPushAggregateThroughOuterJoin4() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "group by e.job";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by on key same as join key, group by on null generating side */
  @Test void testPushAggregateThroughOuterJoin5() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "group by d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by on key same as join key, group by on both side */
  @Test void testPushAggregateThroughOuterJoin6() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job,d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by key is susbset of join keys, group by on non-null generating side */
  @Test void testPushAggregateThroughOuterJoin7() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by key is susbset of join keys, group by on null generating side */
  @Test void testPushAggregateThroughOuterJoin8() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, group by key is susbset of join keys, group by on both sides */
  @Test void testPushAggregateThroughOuterJoin9() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job, d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job, d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * outer join, with aggregate functions */
  @Test void testPushAggregateThroughOuterJoin10() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select count(e.ename)\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "left outer join sales.emp as d on e.job = d.job\n"
        + "group by e.ename,d.mgr";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .checkUnchanged();
  }

  /** Test case for
   * non-equi outer join */
  @Test void testPushAggregateThroughOuterJoin11() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.empno,d.deptno\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "left outer join sales.dept as d on e.empno < d.deptno\n"
        + "group by e.empno,d.deptno";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .checkUnchanged();
  }

  /** Test case for
   * right outer join, group by on key same as join key, group by on (left)null generating side */
  @Test void testPushAggregateThroughOuterJoin12() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "right outer join sales.dept as d on e.job = d.name\n"
        + "group by e.job";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * full outer join, group by on key same as join key, group by on one side */
  @Test void testPushAggregateThroughOuterJoin13() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "full outer join sales.dept as d on e.job = d.name\n"
        + "group by e.job";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * full outer join, group by on key same as join key, group by on both side */
  @Test void testPushAggregateThroughOuterJoin14() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.mgr, d.mgr\n"
        + "from sales.emp as e\n"
        + "full outer join sales.emp as d on e.mgr = d.mgr\n"
        + "group by d.mgr, e.mgr";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * full outer join, group by on both side on non-join keys */
  @Test void testPushAggregateThroughOuterJoin15() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.ename, d.mgr\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "full outer join sales.emp as d on e.job = d.job\n"
        + "group by e.ename,d.mgr";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * full outer join, group by key is susbset of join keys */
  @Test void testPushAggregateThroughOuterJoin16() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "full outer join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  @Test void testPushAggregateThroughJoin2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job,d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job,d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  @Test void testPushAggregateThroughJoin3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.empno,d.deptno\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.empno < d.deptno\n"
        + "group by e.empno,d.deptno";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1544">[CALCITE-1544]
   * AggregateJoinTransposeRule fails to preserve row type</a>. */
  @Test void testPushAggregateThroughJoin4() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.deptno\n"
        + "from sales.emp as e join sales.dept as d on e.deptno = d.deptno\n"
        + "group by e.deptno";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  @Test void testPushAggregateThroughJoin5() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.deptno, d.deptno\n"
        + "from sales.emp as e join sales.dept as d on e.deptno = d.deptno\n"
        + "group by e.deptno, d.deptno";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2200">[CALCITE-2200]
   * Infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test void testJoinPushTransitivePredicatesRule() {
    HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(JoinPushTransitivePredicatesRule.INSTANCE)
        .build();

    final HepProgram emptyProgram = new HepProgramBuilder().build();

    final String sql = "select d.deptno from sales.emp d where d.deptno\n"
        + "IN (select e.deptno from sales.emp e "
        + "where e.deptno = d.deptno or e.deptno = 4)";
    sql(sql).withPre(preProgram).with(emptyProgram).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2205">[CALCITE-2205]
   * One more infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test void testJoinPushTransitivePredicatesRule2() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(JoinPushTransitivePredicatesRule.INSTANCE)
        .build();
    final String sql = "select n1.SAL\n"
        + "from EMPNULLABLES_20 n1\n"
        + "where n1.SAL IN (\n"
        + "  select n2.SAL\n"
        + "  from EMPNULLABLES_20 n2\n"
        + "  where n1.SAL = n2.SAL or n1.SAL = 4)";
    sql(sql).withDecorrelation(true).with(program).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2275">[CALCITE-2275]
   * JoinPushTransitivePredicatesRule wrongly pushes down NOT condition</a>. */
  @Test void testInferringPredicatesWithNotOperatorInJoinCondition() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .addRuleInstance(FilterJoinRule.JOIN)
        .addRuleInstance(JoinPushTransitivePredicatesRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp d\n"
        + "join sales.emp e on e.deptno = d.deptno and d.deptno not in (4, 6)";
    sql(sql).withDecorrelation(true).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2195">[CALCITE-2195]
   * AggregateJoinTransposeRule fails to aggregate over unique column</a>. */
  @Test void testPushAggregateThroughJoin6() {
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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2278">[CALCITE-2278]
   * AggregateJoinTransposeRule fails to split aggregate call if input contains
   * an aggregate call and has distinct rows</a>. */
  @Test void testPushAggregateThroughJoinWithUniqueInput() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select A.job, B.mgr, A.deptno,\n"
        + "max(B.hiredate1) as hiredate1, sum(B.comm1) as comm1\n"
        + "from sales.emp as A\n"
        + "join (select mgr, sal, max(hiredate) as hiredate1,\n"
        + "    sum(comm) as comm1 from sales.emp group by mgr, sal) as B\n"
        + "on A.sal=B.sal\n"
        + "group by A.job, B.mgr, A.deptno";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** SUM is the easiest aggregate function to split. */
  @Test void testPushAggregateSumThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job,sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2105">[CALCITE-2105]
   * AggregateJoinTransposeRule incorrectly makes a SUM NOT NULL when Aggregate
   * has no group keys</a>. */
  @Test void testPushAggregateSumWithoutGroupKeyThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2108">[CALCITE-2108]
   * AggregateJoinTransposeRule incorrectly splits a SUM0 call when Aggregate
   * has no group keys</a>.
   *
   * <p>Similar to {@link #testPushAggregateSumThroughJoin()},
   * but also uses {@link AggregateReduceFunctionsRule}. */
  @Test void testPushAggregateSumThroughJoinAfterAggregateReduce() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateReduceFunctionsRule.INSTANCE)
        .addRuleInstance(AggregateJoinTransposeRule.EXTENDED)
        .build();
    final String sql = "select sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name";
    sql(sql).withPre(preProgram)
        .with(program)
        .check();
  }

  /** Push a variety of aggregate functions. */
  @Test void testPushAggregateFunctionsThroughJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select e.job,\n"
        + "  min(sal) as min_sal, min(e.deptno) as min_deptno,\n"
        + "  sum(sal) + 1 as sum_sal_plus, max(sal) as max_sal,\n"
        + "  sum(sal) as sum_sal_2, count(sal) as count_sal,\n"
        + "  count(mgr) as count_mgr\n"
        + "from sales.emp as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Push a aggregate functions into a relation that is unique on the join
   * key. */
  @Test void testPushAggregateThroughJoinDistinct() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select d.name,\n"
        + "  sum(sal) as sum_sal, count(*) as c\n"
        + "from sales.emp as e\n"
        + "join (select distinct name from sales.dept) as d\n"
        + "  on e.job = d.name\n"
        + "group by d.name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Push count(*) through join, no GROUP BY. */
  @Test void testPushAggregateSumNoGroup() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .build();
    final String sql =
        "select count(*) from sales.emp join sales.dept on job = name";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3076">[CALCITE-3076]
   * AggregateJoinTransposeRule throws error for unique under aggregate keys when
   * generating merged calls</a>.*/
  @Test void testPushAggregateThroughJoinOnEmptyLogicalValues() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ReduceExpressionsRule.FilterReduceExpressionsRule.FILTER_INSTANCE)
        .build();

    final String sql =
        "select count(*) volume, sum(C1.sal) C1_sum_sal "
            + "from (select sal, ename from sales.emp where 1=2) C1 "
            + "inner join (select ename from sales.emp) C2   "
            + "on C1.ename = C2.ename ";
    sql(sql).withPre(preProgram)
        .withRule(AggregateJoinTransposeRule.EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2249">[CALCITE-2249]
   * AggregateJoinTransposeRule generates inequivalent nodes if Aggregate relNode contains
   * distinct aggregate function.</a>. */
  @Test void testPushDistinctAggregateIntoJoin() throws Exception {
    final String sql =
            "select count(distinct sal) from sales.emp join sales.dept on job = name";
    sql(sql).withRule(AggregateJoinTransposeRule.EXTENDED)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate.
   */
  @Test void testAggregateMerge1() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno c, min(y), max(z) z,\n"
        + "sum(r), sum(m) n, sum(x) sal from (\n"
        + "   select deptno, ename, sum(sal) x, max(sal) z,\n"
        + "      min(sal) y, count(hiredate) m, count(mgr) r\n"
        + "   from sales.emp group by deptno, ename) t\n"
        + "group by deptno";
    sql(sql).withPre(preProgram)
        .with(program)
        .check();
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate, top aggregate is not simple aggregate.
   */
  @Test void testAggregateMerge2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, empno, sum(x), sum(y)\n"
        + "from (\n"
        + "  select ename, empno, deptno, sum(sal) x, count(mgr) y\n"
        + "    from sales.emp\n"
        + "  group by deptno, ename, empno) t\n"
        + "group by grouping sets(deptno, empno)";
    sql(sql).withPre(preProgram)
        .with(program)
        .check();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since lower aggregate is not simple aggregate.
   */
  @Test void testAggregateMerge3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, sum(x) from (\n"
        + " select ename, deptno, sum(sal) x from\n"
        + "   sales.emp group by cube(deptno, ename)) t\n"
        + "group by deptno";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since it contains distinct aggregate
   * function.
   */
  @Test void testAggregateMerge4() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, sum(x) from (\n"
        + "  select ename, deptno, count(distinct sal) x\n"
        + "    from sales.emp group by deptno, ename) t\n"
        + "group by deptno";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since AVG doesn't support splitting.
   */
  @Test void testAggregateMerge5() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, avg(x) from (\n"
        + "  select mgr, deptno, avg(sal) x from\n"
        + "    sales.emp group by deptno, mgr) t\n"
        + "group by deptno";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since top agg has no group key, and
   * lower agg function is COUNT.
   */
  @Test void testAggregateMerge6() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateMergeRule.INSTANCE)
        .build();
    final String sql = "select sum(x) from (\n"
        + "select mgr, deptno, count(sal) x from\n"
        + "sales.emp group by deptno, mgr) t";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since top agg contains empty grouping set,
   * and lower agg function is COUNT.
   */
  @Test void testAggregateMerge7() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateMergeRule.INSTANCE)
        .build();
    final String sql = "select mgr, deptno, sum(x) from (\n"
        + "  select mgr, deptno, count(sal) x from\n"
        + "    sales.emp group by deptno, mgr) t\n"
        + "group by cube(mgr, deptno)";
    sql(sql).withPre(preProgram).with(program)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate, since both top and bottom aggregates
   * contains empty grouping set and they are mergable.
   */
  @Test void testAggregateMerge8() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateMergeRule.INSTANCE)
        .build();
    final String sql = "select sum(x) x, min(y) z from (\n"
        + "  select sum(sal) x, min(sal) y from sales.emp)";
    sql(sql).withPre(preProgram)
        .with(program)
        .check();
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and all aggregate functions are splittable.
   */
  @Test void testAggregateRemove1() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateRemoveRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select empno, sum(sal), min(sal), max(sal), "
        + "bit_and(distinct sal), bit_or(sal), count(distinct sal) "
        + "from sales.emp group by empno, deptno\n";
    sql(sql).with(program)
        .check();
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and there are no aggregate functions.
   */
  @Test void testAggregateRemove2() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateRemoveRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select distinct empno, deptno from sales.emp\n";
    sql(sql).with(program)
        .check();
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and all aggregate functions are splittable. Count
   * aggregate function should be transformed to CASE function call
   * because mgr is nullable.
   */
  @Test void testAggregateRemove3() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateRemoveRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select empno, count(mgr) "
        + "from sales.emp group by empno, deptno\n";
    sql(sql).with(program)
        .check();
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove aggregate because avg is not splittable.
   */
  @Test void testAggregateRemove4() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateRemoveRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select empno, max(sal), avg(sal) "
        + "from sales.emp group by empno, deptno\n";
    sql(sql).with(program)
        .checkUnchanged();
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove non-simple aggregates.
   */
  @Test void testAggregateRemove5() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateRemoveRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select empno, deptno, sum(sal) "
        + "from sales.emp group by cube(empno, deptno)\n";
    sql(sql).with(program)
        .checkUnchanged();
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove aggregate because deptno is not unique.
   */
  @Test void testAggregateRemove6() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateRemoveRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = "select deptno, max(sal) "
        + "from sales.emp group by deptno\n";
    sql(sql).with(program)
        .checkUnchanged();
  }

  /**
   * The top Aggregate should be removed -- given "deptno=100",
   * the input of top Aggregate must be already distinct by "mgr"
   */
  @Test void testAggregateRemove7() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateRemoveRule.INSTANCE)
        .addRuleInstance(ProjectMergeRule.INSTANCE)
        .build();
    final String sql = ""
        + "select mgr, sum(sum_sal)\n"
        + "from\n"
        + "(select mgr, deptno, sum(sal) sum_sal\n"
        + " from sales.emp\n"
        + " group by mgr, deptno)\n"
        + "where deptno=100\n"
        + "group by mgr";
    sql(sql)
        .with(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2712">[CALCITE-2712]
   * Should remove the left join since the aggregate has no call and
   * only uses column in the left input of the bottom join as group key.</a>. */
  @Test void testAggregateJoinRemove1() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select distinct e.deptno from sales.emp e\n"
            + "left outer join sales.dept d on e.deptno = d.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but has aggregate
   * call with distinct. */
  @Test void testAggregateJoinRemove2() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select e.deptno, count(distinct e.job) from sales.emp e\n"
            + "left outer join sales.dept d on e.deptno = d.deptno\n"
            + "group by e.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but should not
   * remove the left join since the aggregate uses column in the right
   * input of the bottom join. */
  @Test void testAggregateJoinRemove3() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select e.deptno, count(distinct d.name) from sales.emp e\n"
            + "left outer join sales.dept d on e.deptno = d.deptno\n"
            + "group by e.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but right join. */
  @Test void testAggregateJoinRemove4() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select distinct d.deptno from sales.emp e\n"
            + "right outer join sales.dept d on e.deptno = d.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove2()} but right join. */
  @Test void testAggregateJoinRemove5() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select d.deptno, count(distinct d.name) from sales.emp e\n"
            + "right outer join sales.dept d on e.deptno = d.deptno\n"
            + "group by d.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove3()} but right join. */
  @Test void testAggregateJoinRemove6() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinRemoveRule.INSTANCE)
        .build();
    final String sql =
        "select d.deptno, count(distinct e.job) from sales.emp e\n"
            + "right outer join sales.dept d on e.deptno = d.deptno\n"
            + "group by d.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the bottom join since the aggregate has no aggregate
   * call. */
  @Test void testAggregateJoinRemove7() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT distinct e.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    sql(sql).with(program)
        .check();
  }


  /** Similar to {@link #testAggregateJoinRemove7()} but has aggregate
   * call. */
  @Test void testAggregateJoinRemove8() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno, COUNT(DISTINCT d2.name)\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno\n"
        + "GROUP BY e.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove7()} but use columns in
   * the right input of the top join. */
  @Test void testAggregateJoinRemove9() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT distinct e.deptno, d2.name\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the bottom join since the aggregate uses column in the
   * right input of bottom join. */
  @Test void testAggregateJoinRemove10() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(AggregateProjectMergeRule.INSTANCE)
        .addRuleInstance(AggregateJoinJoinRemoveRule.INSTANCE)
        .build();
    final String sql = "SELECT e.deptno, COUNT(DISTINCT d1.name, d2.name)\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno\n"
        + "GROUP BY e.deptno";
    sql(sql).with(program)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the bottom join since the project uses column in the
   * right input of bottom join. */
  @Test void testProjectJoinRemove1() {
    final String sql = "SELECT e.deptno, d2.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    sql(sql).withRule(ProjectJoinJoinRemoveRule.INSTANCE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the bottom join since the project uses column in the
   * left input of bottom join. */
  @Test void testProjectJoinRemove2() {
    final String sql = "SELECT e.deptno, d1.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    sql(sql).withRule(ProjectJoinJoinRemoveRule.INSTANCE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the bottom join since the right join keys of bottom
   * join are not unique. */
  @Test void testProjectJoinRemove3() {
    final String sql = "SELECT e1.deptno, d.deptno\n"
        + "FROM sales.emp e1\n"
        + "LEFT JOIN sales.emp e2 ON e1.deptno = e2.deptno\n"
        + "LEFT JOIN sales.dept d ON e1.deptno = d.deptno";
    sql(sql).withRule(ProjectJoinJoinRemoveRule.INSTANCE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the left join since the join key of the right input is
   * unique. */
  @Test void testProjectJoinRemove4() {
    final String sql = "SELECT e.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d ON e.deptno = d.deptno";
    sql(sql).withRule(ProjectJoinRemoveRule.INSTANCE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the left join since the join key of the right input is
   * not unique. */
  @Test void testProjectJoinRemove5() {
    final String sql = "SELECT e1.deptno\n"
        + "FROM sales.emp e1\n"
        + "LEFT JOIN sales.emp e2 ON e1.deptno = e2.deptno";
    sql(sql).withRule(ProjectJoinRemoveRule.INSTANCE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the left join since the project use columns in the right
   * input of the join. */
  @Test void testProjectJoinRemove6() {
    final String sql = "SELECT e.deptno, d.name\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d ON e.deptno = d.deptno";
    sql(sql).withRule(ProjectJoinRemoveRule.INSTANCE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the right join since the join key of the left input is
   * unique. */
  @Test void testProjectJoinRemove7() {
    final String sql = "SELECT e.deptno\n"
        + "FROM sales.dept d\n"
        + "RIGHT JOIN sales.emp e ON e.deptno = d.deptno";
    sql(sql).withRule(ProjectJoinRemoveRule.INSTANCE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the right join since the join key of the left input is
   * not unique. */
  @Test void testProjectJoinRemove8() {
    final String sql = "SELECT e2.deptno\n"
        + "FROM sales.emp e1\n"
        + "RIGHT JOIN sales.emp e2 ON e1.deptno = e2.deptno";
    sql(sql).withRule(ProjectJoinRemoveRule.INSTANCE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the right join since the project uses columns in the
   * left input of the join. */
  @Test void testProjectJoinRemove9() {
    final String sql = "SELECT e.deptno, d.name\n"
        + "FROM sales.dept d\n"
        + "RIGHT JOIN sales.emp e ON e.deptno = d.deptno";
    sql(sql).withRule(ProjectJoinRemoveRule.INSTANCE)
        .checkUnchanged();
  }

  @Test void testSwapOuterJoin() {
    final HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(JoinCommuteRule.SWAP_OUTER)
        .build();
    final String sql = "select 1 from sales.dept d left outer join sales.emp e\n"
        + " on d.deptno = e.deptno";
    sql(sql).with(program).check();
  }

  @Test void testPushJoinCondDownToProject() {
    final String sql = "select d.deptno, e.deptno from sales.dept d, sales.emp e\n"
        + " where d.deptno + 10 = e.deptno * 2";
    sql(sql).withRule(FilterJoinRule.FILTER_ON_JOIN,
        JoinPushExpressionsRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinTranspose1() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e left join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by sal limit 10";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinTransposeRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinTranspose2() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by name";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinTransposeRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinTranspose3() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    // This one cannot be pushed down
    final String sql = "select * from sales.emp e left join (\n"
        + "  select * from sales.dept) d on e.deptno = d.deptno\n"
        + "order by sal, name limit 10";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinTransposeRule.INSTANCE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-931">[CALCITE-931]
   * Wrong collation trait in SortJoinTransposeRule for right joins</a>. */
  @Test void testSortJoinTranspose4() {
    // Create a customized test with RelCollation trait in the test cluster.
    Tester tester = new TesterImpl(getDiffRepos(), true, true, false, false,
        true, null, null) {
      @Override public RelOptPlanner createPlanner() {
        return new MockRelOptPlanner(Contexts.empty()) {
          @Override public List<RelTraitDef> getRelTraitDefs() {
            return ImmutableList.of(RelCollationTraitDef.INSTANCE);
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
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by name";
    sql(sql).withTester(t -> tester)
        .withPre(preProgram)
        .withRule(SortJoinTransposeRule.INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1498">[CALCITE-1498]
   * Avoid LIMIT with trivial ORDER BY being pushed through JOIN endlessly</a>. */
  @Test void testSortJoinTranspose5() {
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
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "limit 10";
    sql(sql).withPre(preProgram)
        .with(program)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1507">[CALCITE-1507]
   * OFFSET cannot be pushed through a JOIN if the non-preserved side of outer
   * join is not count-preserving</a>. */
  @Test void testSortJoinTranspose6() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    // This one can be pushed down even if it has an OFFSET, since the dept
    // table is count-preserving against the join condition.
    final String sql = "select d.deptno, empno from sales.dept d\n"
        + "right join sales.emp e using (deptno) limit 10 offset 2";
    sql(sql)
        .withPre(preProgram)
        .withRule(SortJoinTransposeRule.INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1507">[CALCITE-1507]
   * OFFSET cannot be pushed through a JOIN if the non-preserved side of outer
   * join is not count-preserving</a>. */
  @Test void testSortJoinTranspose7() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    // This one cannot be pushed down
    final String sql = "select d.deptno, empno from sales.dept d\n"
        + "left join sales.emp e using (deptno) order by d.deptno offset 1";
    sql(sql)
        .withPre(preProgram)
        .withRule(SortJoinTransposeRule.INSTANCE)
        .checkUnchanged();
  }

  @Test void testSortProjectTranspose1() {
    // This one can be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as integer) offset 1";
    sql(sql).withRule(SortProjectTransposeRule.INSTANCE)
        .check();
  }

  @Test void testSortProjectTranspose2() {
    // This one can be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as double) offset 1";
    sql(sql).withRule(SortProjectTransposeRule.INSTANCE)
        .check();
  }

  @Test void testSortProjectTranspose3() {
    // This one cannot be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as varchar(10)) offset 1";
    sql(sql).withRule(SortJoinTransposeRule.INSTANCE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1023">[CALCITE-1023]
   * Planner rule that removes Aggregate keys that are constant</a>. */
  @Test void testAggregateConstantKeyRule() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal";
    sql(sql).withRule(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .check();
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where reduction is not
   * possible because "deptno" is the only key. */
  @Test void testAggregateConstantKeyRule2() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno";
    sql(sql).withRule(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .checkUnchanged();
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where both keys are
   * constants but only one can be removed. */
  @Test void testAggregateConstantKeyRule3() {
    final String sql = "select job\n"
        + "from sales.emp\n"
        + "where sal is null and job = 'Clerk'\n"
        + "group by sal, job\n"
        + "having count(*) > 3";
    sql(sql).withRule(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .check();
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where
   * there are group keys of type
   * {@link org.apache.calcite.sql.fun.SqlAbstractTimeFunction}
   * that can not be removed. */
  @Test void testAggregateDynamicFunction() {
    final String sql = "select hiredate\n"
        + "from sales.emp\n"
        + "where sal is null and hiredate = current_timestamp\n"
        + "group by sal, hiredate\n"
        + "having count(*) > 3";
    sql(sql).withRule(AggregateProjectPullUpConstantsRule.INSTANCE2)
        .check();
  }

  @Test void testReduceExpressionsNot() {
    final String sql = "select * from (values (false),(true)) as q (col1) where not(col1)";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE)
        .checkUnchanged();
  }

  private Sql checkSubQuery(String sql) {
    return sql(sql).withRule(SubQueryRemoveRule.PROJECT,
        SubQueryRemoveRule.FILTER,
        SubQueryRemoveRule.JOIN)
        .expand(false);
  }

  /** Tests expanding a sub-query, specifically an uncorrelated scalar
   * sub-query in a project (SELECT clause). */
  @Test void testExpandProjectScalar() throws Exception {
    final String sql = "select empno,\n"
        + "  (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql).check();
  }

  @Test void testSelectNotInCorrelated() {
    final String sql = "select sal,\n"
        + " empno NOT IN (\n"
        + " select deptno from dept\n"
        + "   where emp.job=dept.name)\n"
        + " from emp";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1493">[CALCITE-1493]
   * Wrong plan for NOT IN correlated queries</a>. */
  @Test void testWhereNotInCorrelated() {
    final String sql = "select sal from emp\n"
        + "where empno NOT IN (\n"
        + "  select deptno from dept\n"
        + "  where emp.job = dept.name)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereNotInCorrelated2() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno NOT IN\n"
        + "   (select empno from (select ename, empno, sal as r from emp) e2\n"
        + "    where r > 2 and e1.ename= e2.ename)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testAll() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno > ALL (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testSome() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno > SOME (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for testing type created by SubQueryRemoveRule: an
   * ANY sub-query is non-nullable therefore plan should have cast. */
  @Test void testAnyInProjectNonNullable() {
    final String sql = "select name, deptno > ANY (\n"
        + "  select deptno from emp)\n"
        + "from dept";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for testing type created by SubQueryRemoveRule; an
   * ANY sub-query is nullable therefore plan should not have cast. */
  @Test void testAnyInProjectNullable() {
    final String sql = "select deptno, name = ANY (\n"
        + "  select mgr from emp)\n"
        + "from dept";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testSelectAnyCorrelated() {
    final String sql = "select empno > ANY (\n"
        + "  select deptno from dept where emp.job = dept.name)\n"
        + "from emp\n";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereAnyCorrelatedInSelect() {
    final String sql =
        "select * from emp where empno > ANY (\n"
            + "  select deptno from dept where emp.job = dept.name)\n";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testSomeWithEquality() {
    final String sql = "select * from emp e1\n"
        + "  where e1.deptno = SOME (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testSomeWithEquality2() {
    final String sql = "select * from emp e1\n"
        + "  where e1.ename= SOME (select name from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1546">[CALCITE-1546]
   * Sub-queries connected by OR</a>. */
  @Test void testWhereOrSubQuery() {
    final String sql = "select * from emp\n"
        + "where sal = 4\n"
        + "or empno NOT IN (select deptno from dept)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testExpandProjectIn() throws Exception {
    final String sql = "select empno,\n"
        + "  deptno in (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test void testExpandProjectInNullable() throws Exception {
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

  @Test void testExpandProjectInComposite() throws Exception {
    final String sql = "select empno, (empno, deptno) in (\n"
        + "    select empno, deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test void testExpandProjectExists() throws Exception {
    final String sql = "select empno,\n"
        + "  exists (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    checkSubQuery(sql)
        .withProperty(Hook.REL_BUILDER_SIMPLIFY, false)
        .check();
  }

  @Test void testExpandFilterScalar() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where (select deptno from sales.emp where empno < 20)\n"
        + " < (select deptno from sales.emp where empno > 100)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test void testExpandFilterIn() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where deptno in (select deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test void testExpandFilterInComposite() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where (empno, deptno) in (\n"
        + "  select empno, deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  /** An IN filter that requires full 3-value logic (true, false, unknown). */
  @Test void testExpandFilterIn3Value() throws Exception {
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
  @Test void testExpandFilterExists() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    checkSubQuery(sql).check();
  }

  /** An EXISTS filter that can be converted into a semi-join. */
  @Test void testExpandFilterExistsSimple() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  /** An EXISTS filter that can be converted into a semi-join. */
  @Test void testExpandFilterExistsSimpleAnd() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)\n"
        + "and emp.sal < 100";
    checkSubQuery(sql).check();
  }

  @Test void testExpandJoinScalar() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on (select deptno from sales.emp where empno < 20)\n"
        + " < (select deptno from sales.emp where empno > 100)";
    checkSubQuery(sql).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3121">[CALCITE-3121]
   * VolcanoPlanner hangs due to sub-query with dynamic star</a>. */
  @Test void testSubQueryWithDynamicStarHang() {
    String sql = "select n.n_regionkey from (select * from "
        + "(select * from sales.customer) t) n where n.n_nationkey >1";

    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    Tester dynamicTester = createDynamicTester().withDecorrelation(true)
        .withClusterFactory(
            relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    RelRoot root = dynamicTester.convertSqlToRel(sql);

    String planBefore = NL + RelOptUtil.toString(root.rel);
    getDiffRepos().assertEquals("planBefore", "${planBefore}", planBefore);

    PushProjector.ExprCondition exprCondition = expr -> {
      if (expr instanceof RexCall) {
        RexCall call = (RexCall) expr;
        return "item".equals(call.getOperator().getName().toLowerCase(Locale.ROOT));
      }
      return false;
    };
    RuleSet ruleSet =
        RuleSets.ofList(
            FilterProjectTransposeRule.INSTANCE,
            FilterMergeRule.INSTANCE,
            ProjectMergeRule.INSTANCE,
            new ProjectFilterTransposeRule(Project.class, Filter .class,
                RelFactories.LOGICAL_BUILDER, exprCondition),
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits =
        root.rel.getCluster().traitSet()
            .replace(0, EnumerableConvention.INSTANCE);

    RelNode relAfter = program.run(planner, root.rel, toTraits,
        Collections.emptyList(), Collections.emptyList());

    String planAfter = NL + RelOptUtil.toString(relAfter);
    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3188">[CALCITE-3188]
   * IndexOutOfBoundsException in ProjectFilterTransposeRule when executing SELECT COUNT(*)</a>. */
  @Test void testProjectFilterTransposeRuleOnEmptyRowType() {
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    // build a rel equivalent to sql:
    // select `empty` from emp
    // where emp.deptno = 20
    RelNode relNode = relBuilder.scan("EMP")
        .filter(relBuilder
            .equals(
                relBuilder.field(1, 0, "DEPTNO"),
                relBuilder.literal(20)))
        .project(ImmutableList.of())
        .build();

    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(relNode);
    RelNode output = hepPlanner.findBestExp();

    final String planAfter = NL + RelOptUtil.toString(output);
    final DiffRepository diffRepos = getDiffRepos();
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    SqlToRelTestBase.assertValid(output);
  }

  @Disabled("[CALCITE-1045]")
  @Test void testExpandJoinIn() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on emp.deptno in (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Disabled("[CALCITE-1045]")
  @Test void testExpandJoinInComposite() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on (emp.empno, dept.deptno) in (\n"
        + "  select empno, deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Test void testExpandJoinExists() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on exists (select deptno from sales.emp where empno < 20)";
    checkSubQuery(sql).check();
  }

  @Test void testDecorrelateExists() throws Exception {
    final String sql = "select * from sales.emp\n"
        + "where EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1511">[CALCITE-1511]
   * AssertionError while decorrelating query with two EXISTS
   * sub-queries</a>. */
  @Test void testDecorrelateTwoExists() throws Exception {
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
  @Test void testDecorrelateUncorrelatedInAndCorrelatedExists() throws Exception {
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
  @Test void testDecorrelateTwoIn() throws Exception {
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
  @Disabled("[CALCITE-1045]")
  @Test void testDecorrelateTwoScalar() throws Exception {
    final String sql = "select deptno,\n"
        + "  (select min(1) from emp where empno > d.deptno) as i0,\n"
        + "  (select min(0) from emp\n"
        + "    where deptno = d.deptno and ename = 'SMITH') as i1\n"
        + "from dept as d";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereInJoinCorrelated() {
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
  @Test void testWhereInCorrelated() {
    final String sql = "select sal from emp where empno IN (\n"
        + "  select deptno from dept where emp.job = dept.name)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereExpressionInCorrelated() {
    final String sql = "select ename from (\n"
        + "  select ename, deptno, sal + 1 as salPlus from emp) as e\n"
        + "where deptno in (\n"
        + "  select deptno from emp where sal + 1 = e.salPlus)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testWhereExpressionInCorrelated2() {
    final String sql = "select name from (\n"
        + "  select name, deptno, deptno - 10 as deptnoMinus from dept) as d\n"
        + "where deptno in (\n"
        + "  select deptno from emp where sal + 1 = d.deptnoMinus)";
    checkSubQuery(sql).withLateDecorrelation(true).check();
  }

  @Test void testExpandWhereComparisonCorrelated() throws Exception {
    final String sql = "select empno\n"
        + "from sales.emp as e\n"
        + "where sal = (\n"
        + "  select max(sal) from sales.emp e2 where e2.empno = e.empno)";
    checkSubQuery(sql).check();
  }

  @Test void testCustomColumnResolvingInNonCorrelatedSubQuery() {
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

  @Test void testCustomColumnResolvingInCorrelatedSubQuery() {
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

  @Test void testCustomColumnResolvingInCorrelatedSubQuery2() {
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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2744">[CALCITE-2744]
   * RelDecorrelator use wrong output map for LogicalAggregate decorrelate</a>. */
  @Test void testDecorrelateAggWithConstantGroupKey() {
    final String sql = "SELECT * FROM emp A where sal in\n"
        + "(SELECT max(sal) FROM emp B where A.mgr = B.empno group by deptno, 'abc')";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .check();
  }

  /** Test case for CALCITE-2744 for aggregate decorrelate with multi-param agg call
   * but without group key. */
  @Test void testDecorrelateAggWithMultiParamsAggCall() {
    final String sql = "SELECT * FROM (SELECT MYAGG(sal, 1) AS c FROM emp) as m,\n"
        + " LATERAL TABLE(ramp(m.c)) AS T(s)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .checkUnchanged();
  }

  /** Same as {@link #testDecorrelateAggWithMultiParamsAggCall}
   * but with a constant group key. */
  @Test void testDecorrelateAggWithMultiParamsAggCall2() {
    final String sql = "SELECT * FROM "
        + "(SELECT MYAGG(sal, 1) AS c FROM emp group by empno, 'abc') as m,\n"
        + " LATERAL TABLE(ramp(m.c)) AS T(s)";
    sql(sql)
        .withLateDecorrelation(true)
        .withTrim(true)
        .with(HepProgram.builder().build())
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-434">[CALCITE-434]
   * Converting predicates on date dimension columns into date ranges</a>,
   * specifically a rule that converts {@code EXTRACT(YEAR FROM ...) = constant}
   * to a range. */
  @Test void testExtractYearToRange() {
    final String sql = "select *\n"
        + "from sales.emp_b as e\n"
        + "where extract(year from birthdate) = 2014";
    final Context context =
        Contexts.of(new CalciteConnectionConfigImpl(new Properties()));
    sql(sql).withRule(DateRangeRules.FILTER_INSTANCE)
        .withContext(context)
        .check();
  }

  @Test void testExtractYearMonthToRange() {
    final String sql = "select *\n"
        + "from sales.emp_b as e\n"
        + "where extract(year from birthdate) = 2014"
        + "and extract(month from birthdate) = 4";
    final Context context =
        Contexts.of(new CalciteConnectionConfigImpl(new Properties()));
    sql(sql).withRule(DateRangeRules.FILTER_INSTANCE)
        .withContext(context)
        .check();
  }

  @Test void testFilterRemoveIsNotDistinctFromRule() {
    final DiffRepository diffRepos = getDiffRepos();
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode root = builder
        .scan("EMP")
        .filter(
            builder.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
            builder.field("DEPTNO"), builder.literal(20)))
        .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(FilterRemoveIsNotDistinctFromRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testOversimplifiedCaseStatement() {
    String sql = "select * from emp "
        + "where MGR > 0 and "
        + "case when MGR > 0 then deptno / MGR else null end > 1";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE)
        .check();
  }

  /** Test case for
  * <a href="https://issues.apache.org/jira/browse/CALCITE-2726">[CALCITE-2726]
  * ReduceExpressionRule may oversimplify filter conditions containing nulls</a>.
  */
  @Test void testNoOversimplificationBelowIsNull() {
    String sql =
        "select * from emp where ( (empno=1 and mgr=1) or (empno=null and mgr=1) ) is null";
    sql(sql).withRule(ReduceExpressionsRule.FILTER_INSTANCE)
        .check();
  }

  @Test void testExchangeRemoveConstantKeysRule() {
    final DiffRepository diffRepos = getDiffRepos();
    final RelBuilder builder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode root = builder
        .scan("EMP")
        .filter(
        builder.call(SqlStdOperatorTable.EQUALS,
          builder.field("EMPNO"), builder.literal(10)))
        .exchange(RelDistributions.hash(ImmutableList.of(0)))
        .project(builder.field(0), builder.field(1))
        .sortExchange(RelDistributions.hash(ImmutableList.of(0, 1)),
        RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1)))
        .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(ExchangeRemoveConstantKeysRule.EXCHANGE_INSTANCE)
        .addRuleInstance(ExchangeRemoveConstantKeysRule.SORT_EXCHANGE_INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testReduceAverageWithNoReduceSum() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    final String sql = "select name, max(name), avg(deptno), min(name)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testNoReduceAverage() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.noneOf(SqlKind.class);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    String sql = "select name, max(name), avg(deptno), min(name)"
        + " from sales.dept group by name";
    sql(sql).withRule(rule).checkUnchanged();
  }

  @Test void testNoReduceSum() {
    final EnumSet<SqlKind> functionsToReduce = EnumSet.noneOf(SqlKind.class);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    String sql = "select name, sum(deptno)"
            + " from sales.dept group by name";
    sql(sql).withRule(rule).checkUnchanged();
  }

  @Test void testReduceAverageAndVarWithNoReduceStddev() {
    // configure rule to reduce AVG and VAR_POP functions
    // other functions like SUM, STDDEV won't be reduced
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.VAR_POP);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " var_pop(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testReduceAverageAndSumWithNoReduceStddevAndVar() {
    // configure rule to reduce AVG and SUM functions
    // other functions like VAR_POP, STDDEV_POP won't be reduced
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.SUM);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " var_pop(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testReduceAllAggregateFunctions() {
    // configure rule to reduce all used functions
    final EnumSet<SqlKind> functionsToReduce = EnumSet.of(SqlKind.AVG, SqlKind.SUM,
        SqlKind.STDDEV_POP, SqlKind.STDDEV_SAMP, SqlKind.VAR_POP, SqlKind.VAR_SAMP);
    final RelOptRule rule = new AggregateReduceFunctionsRule(LogicalAggregate.class,
        RelFactories.LOGICAL_BUILDER, functionsToReduce);
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " stddev_samp(deptno), var_pop(deptno), var_samp(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  /** Test case for
  * <a href="https://issues.apache.org/jira/browse/CALCITE-2803">[CALCITE-2803]
  * Identify expanded IS NOT DISTINCT FROM expression when pushing project past join</a>.
  */
  @Test void testPushProjectWithIsNotDistinctFromPastJoin() {
    final String sql = "select e.sal + b.comm from emp e inner join bonus b\n"
        + "on (e.ename || e.job) IS NOT DISTINCT FROM (b.ename || b.job) and e.deptno = 10";
    sql(sql).withRule(ProjectJoinTransposeRule.INSTANCE).check();
  }

  @Test void testDynamicStarWithUnion() {
    String sql = "(select n_nationkey from SALES.CUSTOMER) union all\n"
        + "(select n_name from CUSTOMER_MODIFIABLEVIEW)";

    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    Tester dynamicTester = createDynamicTester().withDecorrelation(true)
        .withClusterFactory(
            relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    RelRoot root = dynamicTester.convertSqlToRel(sql);

    String planBefore = NL + RelOptUtil.toString(root.rel);
    getDiffRepos().assertEquals("planBefore", "${planBefore}", planBefore);

    RuleSet ruleSet =
        RuleSets.ofList(
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits =
        root.rel.getCluster().traitSet()
            .replace(0, EnumerableConvention.INSTANCE);

    RelNode relAfter = program.run(planner, root.rel, toTraits,
        Collections.emptyList(), Collections.emptyList());

    String planAfter = NL + RelOptUtil.toString(relAfter);
    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  @Test void testFilterAndProjectWithMultiJoin() throws Exception {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleCollection(Arrays.asList(MyFilterRule.INSTANCE, MyProjectRule.INSTANCE))
        .build();

    final FilterMultiJoinMergeRule filterMultiJoinMergeRule =
        new FilterMultiJoinMergeRule(MyFilter.class, RelFactories.LOGICAL_BUILDER);

    final ProjectMultiJoinMergeRule projectMultiJoinMergeRule =
        new ProjectMultiJoinMergeRule(MyProject.class, RelFactories.LOGICAL_BUILDER);

    HepProgram program = new HepProgramBuilder()
        .addRuleCollection(
            Arrays.asList(
                JoinToMultiJoinRule.INSTANCE,
                filterMultiJoinMergeRule,
                projectMultiJoinMergeRule))
        .build();

    sql("select * from emp e1 left outer join dept d on e1.deptno = d.deptno where d.deptno > 3")
        .withPre(preProgram).with(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3151">[CALCITE-3151]
   * RexCall's Monotonicity is not considered in determining a Calc's collation</a>
   */
  @Test void testMonotonicityUDF() throws Exception {
    final SqlFunction monotonicityFun =
        new SqlFunction("MONOFUN", SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT, null,
            OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION) {
          @Override public boolean isDeterministic() {
            return false;
          }

          @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
            return SqlMonotonicity.INCREASING;
          }
        };

    // Build a tree equivalent to the SQL
    // SELECT sal, MONOFUN() AS n FROM emp
    final RelBuilder builder =
        RelBuilder.create(RelBuilderTest.config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("SAL"),
                builder.alias(builder.call(monotonicityFun), "M"))
            .build();

    HepProgram preProgram = new HepProgramBuilder().build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(root);
    final RelNode relBefore = prePlanner.findBestExp();
    final RelCollation collationBefore =
        relBefore.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    HepProgram hepProgram = new HepProgramBuilder()
        .addRuleInstance(ProjectToCalcRule.INSTANCE)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final RelCollation collationAfter =
        relAfter.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    assertEquals(collationBefore, collationAfter);
  }

  @Test void testPushFiltertWithIsNotDistinctFromPastJoin() {
    String query = "SELECT * FROM "
        + "emp t1 INNER JOIN "
        + "emp t2 "
        + "ON t1.deptno = t2.deptno "
        + "WHERE t1.ename is not distinct from t2.ename";
    sql(query).withRule(FilterJoinRule.FILTER_ON_JOIN).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3997">[CALCITE-3997]
   * Logical rules applied on physical operator but failed handle traits</a>
   */
  @Test void testMergeJoinCollation() {
    final String sql = "select r.ename, s.sal from\n"
        + "sales.emp r join sales.bonus s\n"
        + "on r.ename=s.ename where r.sal+1=s.sal";
    sql(sql, false).check();
  }

  Sql sql(String sql, boolean topDown) {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.setTopDownOpt(topDown);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    RelOptUtil.registerDefaultRules(planner, false, false);
    Tester tester = createTester().withDecorrelation(true)
        .withClusterFactory(cluster -> RelOptCluster.create(planner, cluster.getRexBuilder()));
    return new Sql(tester, sql, null, planner,
        ImmutableMap.of(), ImmutableList.of());
  }

  /**
   * Custom implementation of {@link Filter} for use
   * in test case to verify that {@link FilterMultiJoinMergeRule}
   * can be created with any {@link Filter} and not limited to
   * {@link org.apache.calcite.rel.logical.LogicalFilter}
   */
  private static class MyFilter extends Filter {

    MyFilter(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexNode condition) {
      super(cluster, traitSet, child, condition);
    }

    public MyFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new MyFilter(getCluster(), traitSet, input, condition);
    }

  }

  /**
   * Rule to transform {@link LogicalFilter} into
   * custom MyFilter
   */
  private static class MyFilterRule extends RelOptRule {
    static final MyFilterRule INSTANCE =
        new MyFilterRule(LogicalFilter.class, RelFactories.LOGICAL_BUILDER);

    private MyFilterRule(Class<? extends Filter> clazz,
        RelBuilderFactory relBuilderFactory) {
      super(RelOptRule.operand(clazz, RelOptRule.any()), relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalFilter logicalFilter = call.rel(0);
      final RelNode input = logicalFilter.getInput();
      final MyFilter myFilter = new MyFilter(input.getCluster(), input.getTraitSet(), input,
          logicalFilter.getCondition());
      call.transformTo(myFilter);
    }
  }

  /**
   * Custom implementation of {@link Project} for use
   * in test case to verify that {@link ProjectMultiJoinMergeRule}
   * can be created with any {@link Project} and not limited to
   * {@link org.apache.calcite.rel.logical.LogicalProject}
   */
  private static class MyProject extends Project {
    MyProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
    }

    public MyProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> projects, RelDataType rowType) {
      return new MyProject(getCluster(), traitSet, input, projects, rowType);
    }
  }

  /**
   * Rule to transform {@link LogicalProject} into custom
   * MyProject
   */
  private static class MyProjectRule extends RelOptRule {
    static final MyProjectRule INSTANCE =
        new MyProjectRule(LogicalProject.class, RelFactories.LOGICAL_BUILDER);

    private MyProjectRule(Class<? extends Project> clazz,
        RelBuilderFactory relBuilderFactory) {
      super(RelOptRule.operand(clazz, RelOptRule.any()), relBuilderFactory, null);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalProject logicalProject = call.rel(0);
      final RelNode input = logicalProject.getInput();
      final MyProject myProject = new MyProject(input.getCluster(), input.getTraitSet(), input,
          logicalProject.getChildExps(), logicalProject.getRowType());
      call.transformTo(myProject);
    }
  }

  @Test void testSortJoinCopyInnerJoinOrderBy() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp join sales.dept on\n"
        + "sales.emp.deptno = sales.dept.deptno order by sal";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopyInnerJoinOrderByLimit() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by sal limit 10";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopyInnerJoinOrderByTwoFields() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SortProjectTransposeRule.INSTANCE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(SortJoinCopyRule.INSTANCE)
        .build();
    final String sql = "select * from sales.emp e join  sales.dept d on\n"
        + " e.deptno = d.deptno order by e.sal,d.name";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderBy() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SemiJoinRule.PROJECT)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in\n"
        + " (select e.deptno from sales.emp e) order by d.deptno";
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderByLimitOffset() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SemiJoinRule.PROJECT)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in\n"
        + " (select e.deptno from sales.emp e) order by d.deptno limit 10 offset 2";
    // Do not copy the limit and offset
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderByOffset() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleInstance(SemiJoinRule.PROJECT)
        .build();
    final String sql = "select * from sales.dept d where d.deptno in"
        + " (select e.deptno from sales.emp e) order by d.deptno offset 2";
    // Do not copy the offset
    sql(sql).withPre(preProgram)
        .withRule(SortJoinCopyRule.INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3296">[CALCITE-3296]
   * Decorrelator gives empty result
   * after decorrelating sort rel with null offset and fetch </a>
   */
  @Test void testDecorrelationWithSort() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)\n"
        + "order by e1.empno";

    sql(sql).with(HepProgram.builder().build())
        .withDecorrelation(true)
        .checkUnchanged();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3319">[CALCITE-3319]
   * AssertionError for ReduceDecimalsRule </a>
   */
  @Test void testReduceDecimal() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(FilterToCalcRule.INSTANCE)
        .addRuleInstance(ReduceDecimalsRule.INSTANCE)
        .build();
    final String sql = "select ename from emp where sal > cast (100.0 as decimal(4, 1))";
    sql(sql).with(program).check();
  }

  @Test void testEnumerableCalcRule() {
    final String sql = "select FNAME, LNAME from SALES.CUSTOMER where CONTACTNO > 10";
    VolcanoPlanner planner = new VolcanoPlanner(null, null);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);

    Tester dynamicTester = createDynamicTester().withDecorrelation(true)
        .withClusterFactory(
            relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    RelRoot root = dynamicTester.convertSqlToRel(sql);

    String planBefore = NL + RelOptUtil.toString(root.rel);
    getDiffRepos().assertEquals("planBefore", "${planBefore}", planBefore);

    RuleSet ruleSet =
        RuleSets.ofList(
            FilterToCalcRule.INSTANCE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_CALC_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits =
        root.rel.getCluster().traitSet()
            .replace(0, EnumerableConvention.INSTANCE);

    RelNode relAfter = program.run(planner, root.rel, toTraits,
        Collections.emptyList(), Collections.emptyList());

    String planAfter = NL + RelOptUtil.toString(relAfter);
    getDiffRepos().assertEquals("planAfter", "${planAfter}", planAfter);
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3404">[CALCITE-3404]
   * Treat agg expressions that can ignore distinct constraint as distinct
   * in AggregateExpandDistinctAggregatesRule
   * when all the other agg expressions are distinct and have same arguments</a>
   */
  @Test void testMaxReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "max(deptno) from emp";
    sql(sql).withRule(AggregateExpandDistinctAggregatesRule.INSTANCE).check();
  }

  @Test void testMinReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "min(deptno) from emp";
    sql(sql).withRule(AggregateExpandDistinctAggregatesRule.INSTANCE).check();
  }

  @Test void testBitAndReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "bit_and(deptno) from emp";
    sql(sql).withRule(AggregateExpandDistinctAggregatesRule.INSTANCE).check();
  }

  @Test void testBitOrReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "bit_or(deptno) from emp";
    sql(sql).withRule(AggregateExpandDistinctAggregatesRule.INSTANCE).check();
  }

  @Test void testProjectJoinTransposeItem() {
    ProjectJoinTransposeRule projectJoinTransposeRule =
        new ProjectJoinTransposeRule(Project.class, Join.class, skipItem, RelFactories
          .LOGICAL_BUILDER);

    String query = "select t1.c_nationkey[0], t2.c_nationkey[0] "
        + "from sales.customer as t1 left outer join sales.customer as t2 "
        + "on t1.c_nationkey[0] = t2.c_nationkey[0]";

    sql(query).withTester(t -> createDynamicTester()).withRule(projectJoinTransposeRule).check();
  }

  @Test void testSimplifyItemIsNotNull() {
    String query = "select * from sales.customer as t1 where t1.c_nationkey[0] is not null";

    sql(query)
        .withTester(t -> createDynamicTester())
        .withRule(ReduceExpressionsRule.FILTER_INSTANCE)
        .checkUnchanged();
  }

  @Test void testSimplifyItemIsNull() {
    String query = "select * from sales.customer as t1 where t1.c_nationkey[0] is null";

    sql(query)
        .withTester(t -> createDynamicTester())
        .withRule(ReduceExpressionsRule.FILTER_INSTANCE)
        .checkUnchanged();
  }

}
