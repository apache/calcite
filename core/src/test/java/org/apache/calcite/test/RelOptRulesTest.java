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

import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableLimitSort;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateExpandWithinDistinctRule;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rel.rules.AggregateProjectConstantToDummyJoinRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.CoerceInputsRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterFlattenCorrelatedConditionRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMultiJoinMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MeasureRules;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectCorrelateTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule.ProjectReduceExpressionsRule;
import org.apache.calcite.rel.rules.SingleValuesOptimizationRules;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.SpatialRules;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.test.SqlToRelTestBase.CustomCorrelate;
import org.apache.calcite.test.catalog.MockCatalogReader;
import org.apache.calcite.test.catalog.MockCatalogReaderExtended;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.calcite.test.SqlToRelTestBase.NL;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
 * {@code build/diffrepo/test/org/apache/calcite/test/RelOptRulesTest_actual.xml},
 * e.g.
 *
 * <blockquote>{@code
 * diff core/src/test/resources/org/apache/calcite/test/RelOptRulesTest.xml
 * build/diffrepo/test/org/apache/calcite/test/RelOptRulesTest_actual.xml
 * }</blockquote>
 *
 * <li>Verify that the "planBefore" is the correct translation of your SQL,
 * and that it contains the pattern on which your rule is supposed to fire.
 * If all is well, replace {@code src/test/resources/.../RelOptRulesTest.xml}
 * with the new {@code build/resources/.../RelOptRulesTest_actual.xml}.
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

  @Nullable
  private static DiffRepository diffRepos = null;

  @AfterAll
  public static void checkActualAndReferenceFiles() {
    if (diffRepos != null) {
      diffRepos.checkActualAndReferenceFiles();
    }
  }

  @Override RelOptFixture fixture() {
    RelOptFixture fixture = super.fixture()
        .withDiffRepos(DiffRepository.lookup(RelOptRulesTest.class));
    diffRepos = fixture.diffRepos();
    return fixture;
  }

  private static boolean skipItem(RexNode expr) {
    return expr instanceof RexCall
          && "item".equalsIgnoreCase(((RexCall) expr).getOperator().getName());
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5813">[CALCITE-5813]
   * Type inference for sql functions REPEAT, SPACE, XML_TRANSFORM,
   * and XML_EXTRACT is incorrect</a>. */
  @Test void testRepeat() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "select REPEAT('abc', 2)";
    fixture()
        .withFactory(
            t -> t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.BIG_QUERY))) // needed for REPEAT function
        .sql(sql)
        .withPlanner(hepPlanner)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5813">[CALCITE-5813]
   * Type inference for sql functions REPEAT, SPACE, XML_TRANSFORM,
   * and XML_EXTRACT is incorrect</a>. */
  @Test void testReplace() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "select REPLACE('abc', 'c', 'cd')";
    fixture()
        .sql(sql)
        .withPlanner(hepPlanner)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5989">[CALCITE-5989]
   * Type inference for RPAD and LPAD functions (BIGQUERY) is incorrect</a>. */
  @Test void testRpad() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "select RPAD('abc', 8, 'A')";
    fixture()
        .withFactory(
            t -> t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.BIG_QUERY))) // needed for RPAD function
        .sql(sql)
        .withPlanner(hepPlanner)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5989">[CALCITE-5989]
   * Type inference for RPAD and LPAD functions (BIGQUERY) is incorrect</a>. */
  @Test void testLpad() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "select LPAD('abc', 8, 'A')";
    fixture()
        .withFactory(
            t -> t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.BIG_QUERY))) // needed for LPAD function
        .sql(sql)
        .withPlanner(hepPlanner)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5971">[CALCITE-5971]
   * Add the RelRule to rewrite the bernoulli sample as Filter</a>. */
  @Test void testSampleToFilter() {
    final String sql = "select deptno from emp tablesample bernoulli(50)";
    sql(sql)
        .withRule(CoreRules.SAMPLE_TO_FILTER)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6317">
   * [CALCITE-6317] Incorrect constant replacement when group keys are NULL</a>. */
  @Test void testPredicatePull() {
    final String sql = "select deptno, sal "
                 + "from emp "
                + "where deptno = 10 "
                + "group by rollup(sal, deptno)";
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5971">[CALCITE-5971]
   * Add the RelRule to rewrite the bernoulli sample as Filter</a>. */
  @Test void testSampleToFilterWithSeed() {
    final String sql = "select deptno from emp tablesample bernoulli(50) REPEATABLE(10)";
    sql(sql)
        .withRule(CoreRules.SAMPLE_TO_FILTER)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6887">[CALCITE-6887]
   * ReduceExpressionsRule applied to 'IN subquery' should make the values distinct
   * if the subquery is a Values composed of literals</a>. */
  @Test void testAggregateValuesRuleWithInRepeatingValues() {
    final String sql = "SELECT deptno, sal "
        + "FROM EMP "
        + "WHERE deptno IN (1,1,3,1,2,1,1,1,1,1,1,1,null,null,1,1,1,1,1,1,1)";

    sql(sql)
        .withPreRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withRule(CoreRules.AGGREGATE_VALUES)
        .check();
  }

  @Test void testReduceInValuesNonOrdered() {
    final String sql = "SELECT deptno, sal "
        + "FROM EMP "
        + "WHERE deptno IN (1,1,3,1,2)";

    sql(sql)
        .withPreRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withRule(CoreRules.AGGREGATE_VALUES)
        .checkUnchanged();
  }

  @Test void testAggregateValuesRuleWithRepeatingValues() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .values(new String[]{"v"}, 1, 1, 3, null, null)
        .distinct()
        .build();
    relFn(relFn)
        .withRule(CoreRules.AGGREGATE_VALUES)
        .check();
  }

  @Test void testAggregateValuesRuleWithAggregation() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .values(new String[]{"v1", "v2"}, 1, "a", 2, "b", 1, "a", 1, "b")
        .aggregate(b.groupKey(ImmutableBitSet.of(0, 1)),
            b.count(b.field(0)).as("V1_COUNT"))
        .build();
    relFn(relFn)
        .withRule(CoreRules.AGGREGATE_VALUES)
        .checkUnchanged();
  }

  @Test void testAggregateValuesRuleWithSameRowType() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .values(new String[]{"v1", "v2"}, 1, "a", 2, "b", 1, "a", 1, "b")
        .aggregate(b.groupKey(ImmutableBitSet.of(0, 1)))
        .build();
    relFn(relFn)
        .withRule(CoreRules.AGGREGATE_VALUES)
        .check();
  }

  @Test void testAggregateValuesRuleWithDifferentRowTypes() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .values(new String[]{"v1", "v2"}, 1, "a", 2, "b", 1, "a", 1, "b")
        .aggregate(b.groupKey(ImmutableBitSet.of(0)))
        .build();
    relFn(relFn)
        .withRule(CoreRules.AGGREGATE_VALUES)
        .checkUnchanged();
  }

  /**
   * Test case for <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6746">
   * [CALCITE-6746] Optimization rule ProjectWindowTranspose is unsound</a>. */
  @Test void testConstantWindow() {
    final String sql = "with empsalary(dept, empno, salary, enroll_date) as "
        + "(VALUES ('x', 10, 5200, DATE '2007-08-01'), (NULL, NULL, NULL, NULL))\n"
        + "select sum(salary) "
        + "OVER (order by enroll_date range between INTERVAL 365 DAYS preceding and "
        + "INTERVAL 365 DAYS following),\n"
        + "salary, enroll_date FROM empsalary";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .withRule(CoreRules.PROJECT_WINDOW_TRANSPOSE)
        .checkUnchanged();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5813">[CALCITE-5813]
   * Type inference for sql functions REPEAT, SPACE, XML_TRANSFORM,
   * and XML_EXTRACT is incorrect</a>. */
  @Test void testSpace() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "select SPACE(2001)";
    fixture()
        .withFactory(
            t -> t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.MYSQL))) // needed for SPACE function
        .sql(sql)
        .withPlanner(hepPlanner)
        .check();
  }

  @Test void testGroupByDateLiteralSimple() {
    final String query = "select avg(sal)\n"
        + "from emp\n"
        + "group by DATE '2022-01-01'";
    sql(query)
        .withRule(AggregateProjectConstantToDummyJoinRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testGroupByBooleanLiteralSimple() {
    final String query = "select avg(sal)\n"
        + "from emp\n"
        + "group by true";
    sql(query)
        .withRule(AggregateProjectConstantToDummyJoinRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testGroupByMultipleLiterals() {
    final String query = "select avg(sal)\n"
        + "from emp\n"
        + "group by false, deptno, true, true, empno, false, 'ab', DATE '2022-01-01'";
    sql(query)
        .withRule(AggregateProjectConstantToDummyJoinRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testReduceNot() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);

    final String sql = "select *\n"
        + "from (select (case when sal > 1000 then null else false end) as caseCol from emp)\n"
        + "where NOT(caseCol)";
    sql(sql).withPlanner(hepPlanner)
        .checkUnchanged();
  }

  @Test void testReduceNestedCaseWhen() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);

    final String sql = "select sal\n"
            + "from emp\n"
            + "where case when (sal = 1000) then\n"
            + "(case when sal = 1000 then null else 1 end is null) else\n"
            + "(case when sal = 2000 then null else 1 end is null) end is true";
    sql(sql).withPlanner(hepPlanner)
        .check();
  }

  @Test void testDigestOfApproximateDistinctAggregateCall() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(AggregateProjectMergeRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.AGGREGATE_PROJECT_MERGE);

    final String sql = "select *\n"
            + "from (\n"
            + "select deptno, count(distinct empno) from emp group by deptno\n"
            + "union all\n"
            + "select deptno, approx_count_distinct(empno) from emp group by deptno)";
    sql(sql).withPlanner(hepPlanner)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1479">[CALCITE-1479]
   * AssertionError in ReduceExpressionsRule on multi-column IN
   * sub-query</a>. */
  @Test void testReduceCompositeInSubQuery() {
    final String sql = "select *\n"
        + "from emp\n"
        + "where (empno, deptno) in (\n"
        + "  select empno, deptno from (\n"
        + "    select empno, deptno\n"
        + "    from emp\n"
        + "    group by empno, deptno))\n"
        + "or deptno < 40 + 60";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderConfig(b -> b.withAggregateUnique(true))
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2865">[CALCITE-2865]
   * FilterProjectTransposeRule generates wrong traitSet when
   * copyFilter/copyProject is true</a>. */
  @Test void testFilterProjectTransposeRule() {
    List<RelOptRule> rules =
        Arrays.asList(
            // default: copyFilter=true, copyProject=true
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE.config
                .withOperandFor(Filter.class,
                    filter -> !RexUtil.containsCorrelation(filter.getCondition()),
                    Project.class, project -> true)
                .withCopyFilter(false)
                .withCopyProject(false)
                .toRule());

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
      RelCollation collation =
          filter.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
      assertNotNull(collation);
      List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
      assertThat(fieldCollations, hasSize(1));
      RelFieldCollation fieldCollation = fieldCollations.get(0);
      assertThat(fieldCollation.getFieldIndex(), is(3));
      assertThat(fieldCollation.getDirection(),
          is(RelFieldCollation.Direction.DESCENDING));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6873">[CALCITE-6873]
   * FilterProjectTransposeRule should not push the Filter past the Project
   * when the Filter contains a Subquery with correlation</a>. */
  @Test void testFilterProjectTransposeRule2() {
    final String sql = "select * from (select deptno from emp) as d\n"
        + "where NOT EXISTS (\n"
        + "  select count(*) from emp e where e.deptno = d.deptno)";
    sql(sql)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .checkUnchanged();
  }

  @Test void testFilterProjectTransposeRule3() {
    final String sql = "select * from (select deptno from emp) as d\n"
        + "where NOT EXISTS (\n"
        + "  select count(*) from emp e)";
    sql(sql)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6031">[CALCITE-6031]
   * Add the planner rule that pushes the Filter past a Sample</a>. */
  @Test void testFilterSampleTransposeWithBernoulli() {
    final String sql = "select deptno from emp tablesample bernoulli(50) where deptno > 10";
    sql(sql)
        .withRule(CoreRules.FILTER_SAMPLE_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6031">[CALCITE-6031]
   * Add the planner rule that pushes the Filter past a Sample</a>. */
  @Test void testFilterSampleTransposeWithSystem() {
    final String sql = "select deptno from emp tablesample system(50) where deptno > 10";
    sql(sql)
        .withRule(CoreRules.FILTER_SAMPLE_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6031">[CALCITE-6031]
   * Add the planner rule that pushes the Filter past a Sample</a>. */
  @Test void testFilterSampleTransposeWithSystemAndSeed() {
    final String sql = "select deptno from emp\n"
        + "tablesample system(50) repeatable(10) where deptno > 10";
    sql(sql)
        .withRule(CoreRules.FILTER_SAMPLE_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6878">[CALCITE-6878]
   * Implement FilterSortTransposeRule</a>. */
  @Test void testFilterSortTranspose() {
    final Function<RelBuilder, RelNode> relFn = b -> b
            .scan("EMP")
            .project(b.field(0))
            .sort(b.field(0))
            .filter(b.lessThan(b.field(0), b.literal(10)))
            .build();
    relFn(relFn).withRule(CoreRules.FILTER_SORT_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6878">[CALCITE-6878]
   * Implement FilterSortTransposeRule</a>. */
  @Test void testFilterSortTransposeFetch() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .project(b.field(0))
        .sortLimit(0, 1, b.field(0))
        .filter(b.lessThan(b.field(0), b.literal(10)))
        .build();
    relFn(relFn).withRule(CoreRules.FILTER_SORT_TRANSPOSE).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6878">[CALCITE-6878]
   * Implement FilterSortTransposeRule</a>. */
  @Test void testFilterSortTransposeOffset() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .project(b.field(0))
        .sortLimit(1, 0, b.field(0))
        .filter(b.lessThan(b.field(0), b.literal(10)))
        .build();
    relFn(relFn).withRule(CoreRules.FILTER_SORT_TRANSPOSE).checkUnchanged();
  }

  @Test void testReduceOrCaseWhen() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);

    final String sql = "select sal\n"
        + "from emp\n"
        + "where case when sal = 1000 then null else 1 end is null\n"
        + "OR case when sal = 2000 then null else 1 end is null";
    sql(sql).withPlanner(hepPlanner)
        .check();
  }

  @Test void testReduceNullableCase() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "SELECT CASE WHEN 1=2 "
        + "THEN cast((values(1)) as integer) "
        + "ELSE 2 end from (values(1))";
    sql(sql)
        .withExpand(true)
        .withPlanner(hepPlanner)
        .checkUnchanged();
  }

  @Test void testReduceNullableCase2() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    final String sql = "SELECT deptno, ename, CASE WHEN 1=2 "
        + "THEN substring(ename, 1, cast(2 as int)) ELSE NULL end from emp"
        + " group by deptno, ename, case when 1=2 then substring(ename,1, cast(2 as int))  else null end";
    sql(sql)
        .withExpand(true)
        .withPlanner(hepPlanner)
        .checkUnchanged();
  }

  @Test void testProjectToWindowRuleForMultipleWindows() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectToWindowRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    final String sql = "select\n"
        + " count(*) over(partition by empno order by sal) as count1,\n"
        + " count(*) over(partition by deptno order by sal) as count2,\n"
        + " sum(deptno) over(partition by empno order by sal) as sum1,\n"
        + " sum(deptno) over(partition by deptno order by sal) as sum2\n"
        + "from emp";
    sql(sql).withPlanner(hepPlanner)
        .check();
  }

  @Test void testUnionToDistinctRule() {
    final String sql = "select * from dept union select * from dept";
    sql(sql).withRule(CoreRules.UNION_TO_DISTINCT).check();
  }

  @Test void testExtractJoinFilterRule() {
    final String sql = "select 1 from emp inner join dept on emp.deptno=dept.deptno";
    sql(sql).withRule(CoreRules.JOIN_EXTRACT_FILTER).check();
  }

  @Test void testNotPushExpression() {
    final String sql = "select 1 from emp inner join dept\n"
        + "on emp.deptno=dept.deptno and emp.ename is not null";
    sql(sql).withRule(CoreRules.JOIN_PUSH_EXPRESSIONS)
        .withRelBuilderSimplify(false)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6011">[CALCITE-6011]
   * Add the planner rule that pushes the Filter past a Window</a>. */
  @Test void testNotPushFilterPastWindowWhenPredicateNotOnPartitionKey() {
    final String sql = "select * from\n"
        + "(select NAME, DEPTNO, count(*) over (partition by NAME) from dept) t\n"
        + "where DEPTNO = 0";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .withRule(
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_WINDOW_TRANSPOSE,
            CoreRules.PROJECT_REMOVE).check();
  }

  @Test void testAddRedundantSemiJoinRule() {
    final String sql = "select 1 from emp inner join dept on emp.deptno = dept.deptno";
    sql(sql).withRule(CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3890">[CALCITE-3890]
   * Derive IS NOT NULL predicates from a inner join and create filters with those predicates
   * as new inputs of the join</a>. */
  @Test void testJoinDeriveIsNotNullFilterRule1() {
    final String sql = "select t1.deptno from emp t1 inner join emp t2 on t1.mgr = t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).check();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should create IS NOT NULL filter for the right input of join, but not for
   * the left input since its child already has one. */
  @Test void testJoinDeriveIsNotNullFilterRule2() {
    final String sql = "select t1.deptno from (select * from emp where mgr is not null) t1 "
        + "join emp t2 on t1.mgr = t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).check();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should create IS NOT NULL filter in both sides. */
  @Test void testJoinDeriveIsNotNullFilterRule3() {
    final String sql = "select t1.deptno from emp t1 inner join emp t2 on t1.mgr > t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).check();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should create IS NOT NULL filter for both join keys. */
  @Test void testJoinDeriveIsNotNullFilterRule4() {
    final String sql = "select t1.deptno from empnullables t1 inner join\n"
        + "empnullables t2 on t1.ename = t2.ename and t1.mgr > t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).check();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should create IS NOT NULL filter for the right input of join, but not for
   * the left input since its ancestor already has one. */
  @Test void testJoinDeriveIsNotNullFilterRule5() {
    final String sql = "select t1.deptno from (select ename, deptno+1 as deptno from\n"
        + "empnullables where ename is not null) t1 inner join\n"
        + "empnullables t2 on t1.ename = t2.ename";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).check();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should not create IS NOT NULL filter in any side since it is full join. */
  @Test void testJoinDeriveIsNotNullFilterRule6() {
    final String sql = "select t1.deptno from emp t1 full join emp t2 on t1.mgr = t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).checkUnchanged();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should not create IS NOT NULL filter in any side since it is left join. */
  @Test void testJoinDeriveIsNotNullFilterRule7() {
    final String sql = "select t1.deptno from emp t1 left join emp t2 on t1.mgr = t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).checkUnchanged();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should not create IS NOT NULL filter in any side since it is right join. */
  @Test void testJoinDeriveIsNotNullFilterRule8() {
    final String sql = "select t1.deptno from emp t1 right join emp t2 on t1.mgr = t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).checkUnchanged();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should not create IS NOT NULL filter in both sides since they already have the filter. */
  @Test void testJoinDeriveIsNotNullFilterRule9() {
    final String sql = "select t1.deptno from (select * from emp where mgr is not null) t1\n"
        + "join (select * from emp where mgr is not null) t2 on t1.mgr = t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).checkUnchanged();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should not create IS NOT NULL filter in any side since the join condition is not strong. */
  @Test void testJoinDeriveIsNotNullFilterRule10() {
    final String sql = "select t1.deptno from emp t1 inner join emp t2\n"
        + "on t1.mgr is not distinct from t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).checkUnchanged();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should not create IS NOT NULL filter since it's cartesian product. */
  @Test void testJoinDeriveIsNotNullFilterRule11() {
    final String sql = "select t1.deptno from empnullables t1 inner join\n"
        + "empnullables t2 on t1.ename = t2.ename or 1 = 1";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).checkUnchanged();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should create IS NOT NULL filter for one of the join keys. */
  @Test void testJoinDeriveIsNotNullFilterRule12() {
    final String sql = "select t1.deptno from empnullables t1 inner join\n"
        + "empnullables t2 on t1.ename = t2.ename and t1.mgr is not distinct from t2.mgr";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).check();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should not create IS NOT NULL filter if join condition is not strong wrt
   * each key. */
  @Test void testJoinDeriveIsNotNullFilterRule13() {
    final String sql = "select t1.deptno from empnullables t1 inner join\n"
        + "empnullables t2 on coalesce(t1.ename, t2.ename) = 'abc'";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).checkUnchanged();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should not create IS NOT NULL filter if join condition is not strong wrt
   * each key. */
  @Test void testJoinDeriveIsNotNullFilterRule14() {
    final String sql = "select t1.deptno from empnullables t1 inner join\n"
        + "empnullables t2 on nvl(t1.ename, t2.ename) = 'abc'";
    sql(sql)
        .withFactory(t ->
            t.withOperatorTable(opTab -> SqlValidatorTest.operatorTableFor(SqlLibrary.ORACLE)))
        .withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).checkUnchanged();
  }

  /** As {@link #testJoinDeriveIsNotNullFilterRule1()};
   * should create IS NOT NULL filter only for the first operand of NULLIF. */
  @Test void testJoinDeriveIsNotNullFilterRule15() {
    final String sql = "select t1.deptno from empnullables t1 inner join\n"
        + "empnullables t2 on nullif(t1.ename, t2.ename) = 'abc'";
    sql(sql).withRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE).check();
  }

  @Test void testStrengthenJoinType() {
    // The "Filter(... , right.c IS NOT NULL)" above a left join is pushed into
    // the join, makes it an inner join, and then disappears because c is NOT
    // NULL.
    final String sql = "select *\n"
        + "from dept left join emp on dept.deptno = emp.deptno\n"
        + "where emp.deptno is not null and emp.sal > 100";
    sql(sql)
        .withDecorrelate(true)
        .withTrim(true)
        .withPreRule(CoreRules.PROJECT_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE)
        .withRule(CoreRules.FILTER_INTO_JOIN)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3170">[CALCITE-3170]
   * ANTI join on conditions push down generates wrong plan</a>. */
  @Test void testCanNotPushAntiJoinConditionsToLeft() {
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // not in (select dept.deptno from dept where emp.deptno > 20)
    checkCanNotPushSemiOrAntiJoinConditionsToLeft(JoinRelType.ANTI);
  }

  @Test void testCanNotPushAntiJoinConditionsToRight() {
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // not in (select dept.deptno from dept where dept.dname = 'ddd')
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT")
        .antiJoin(
            b.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO")),
            b.equals(b.field(2, 1, "DNAME"),
                b.literal("ddd")))
        .project(b.field(0))
        .build();
    relFn(relFn).withRule(CoreRules.JOIN_CONDITION_PUSH).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3171">[CALCITE-3171]
   * SemiJoin on conditions push down throws IndexOutOfBoundsException</a>. */
  @Test void testPushSemiJoinConditionsToLeft() {
    // build a rel equivalent to sql:
    // select * from emp
    // where emp.deptno
    // in (select dept.deptno from dept where emp.empno > 20)
    checkCanNotPushSemiOrAntiJoinConditionsToLeft(JoinRelType.SEMI);
  }

  private void checkCanNotPushSemiOrAntiJoinConditionsToLeft(JoinRelType type) {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode left = b.scan("EMP").build();
      RelNode right = b.scan("DEPT").build();
      return b.push(left)
          .push(right)
          .join(type,
              b.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")),
              b.greaterThan(RexInputRef.of(0, left.getRowType()),
                  b.literal(20)))
          .project(b.field(0))
          .build();
    };
    relFn(relFn).withRule(CoreRules.JOIN_PUSH_EXPRESSIONS).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3979">[CALCITE-3979]
   * ReduceExpressionsRule might have removed CAST expression(s) incorrectly</a>. */
  @Test void testCastRemove() {
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
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3887">[CALCITE-3887]
   * Filter and Join conditions may not need to retain nullability during simplifications</a>. */
  @Test void testPushSemiJoinConditions() {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode left = b.scan("EMP")
          .project(
              b.field("DEPTNO"),
              b.field("ENAME"))
          .build();
      RelNode right = b.scan("DEPT")
          .project(
              b.field("DEPTNO"),
              b.field("DNAME"))
          .build();

      b.push(left).push(right);

      RexInputRef ref1 = b.field(2, 0, "DEPTNO");
      RexInputRef ref2 = b.field(2, 1, "DEPTNO");
      RexInputRef ref3 = b.field(2, 0, "ENAME");
      RexInputRef ref4 = b.field(2, 1, "DNAME");

      // ref1 IS NOT DISTINCT FROM ref2
      RexCall cond1 =
          (RexCall) b.call(SqlStdOperatorTable.OR, b.equals(ref1, ref2),
              b.call(SqlStdOperatorTable.AND, b.isNull(ref1), b.isNull(ref2)));

      // ref3 IS NOT DISTINCT FROM ref4
      RexCall cond2 =
          (RexCall) b.call(SqlStdOperatorTable.OR, b.equals(ref3, ref4),
              b.call(SqlStdOperatorTable.AND, b.isNull(ref3), b.isNull(ref4)));

      RexNode cond = b.and(cond1, cond2);
      return b.semiJoin(cond)
          .project(b.field(0))
          .build();
    };

    relFn(relFn)
        .withRule(
            CoreRules.JOIN_PUSH_EXPRESSIONS,
            CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE,
            CoreRules.JOIN_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testFullOuterJoinSimplificationToLeftOuter() {
    final String sql = "select 1 from sales.dept d full outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where d.name = 'Charlie'";
    sql(sql).withRule(CoreRules.FILTER_INTO_JOIN).check();
  }

  @Test void testFullOuterJoinSimplificationToRightOuter() {
    final String sql = "select 1 from sales.dept d full outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where e.sal > 100";
    sql(sql).withRule(CoreRules.FILTER_INTO_JOIN).check();
  }

  @Test void testFullOuterJoinSimplificationToInner() {
    final String sql = "select 1 from sales.dept d full outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where d.name = 'Charlie' and e.sal > 100";
    sql(sql).withRule(CoreRules.FILTER_INTO_JOIN).check();
  }

  @Test void testLeftOuterJoinSimplificationToInner() {
    final String sql = "select 1 from sales.dept d left outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where e.sal > 100";
    sql(sql).withRule(CoreRules.FILTER_INTO_JOIN).check();
  }

  @Test void testRightOuterJoinSimplificationToInner() {
    final String sql = "select 1 from sales.dept d right outer join sales.emp e\n"
        + "on d.deptno = e.deptno\n"
        + "where d.name = 'Charlie'";
    sql(sql).withRule(CoreRules.FILTER_INTO_JOIN).check();
  }

  @Test void testPushAboveFiltersIntoInnerJoinCondition() {
    final String sql = ""
        + "select * from sales.dept d inner join sales.emp e\n"
        + "on d.deptno = e.deptno and d.deptno > e.mgr\n"
        + "where d.deptno > e.mgr";
    sql(sql).withRule(CoreRules.FILTER_INTO_JOIN).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3225">[CALCITE-3225]
   * JoinToMultiJoinRule should not match SEMI/ANTI LogicalJoin</a>. */
  @Test void testJoinToMultiJoinDoesNotMatchSemiJoin() {
    // build a rel equivalent to sql:
    // select * from
    // (select * from emp join dept ON emp.deptno = emp.deptno) t
    // where emp.job in (select job from bonus)
    checkJoinToMultiJoinDoesNotMatchSemiOrAntiJoin(JoinRelType.SEMI);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3225">[CALCITE-3225]
   * JoinToMultiJoinRule should not match SEMI/ANTI LogicalJoin</a>. */
  @Test void testJoinToMultiJoinDoesNotMatchAntiJoin() {
    // build a rel equivalent to sql:
    // select * from
    // (select * from emp join dept ON emp.deptno = emp.deptno) t
    // where not exists (select job from bonus where emp.job = bonus.job)
    checkJoinToMultiJoinDoesNotMatchSemiOrAntiJoin(JoinRelType.ANTI);
  }

  private void checkJoinToMultiJoinDoesNotMatchSemiOrAntiJoin(JoinRelType type) {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode left = b.scan("EMP").build();
      RelNode right = b.scan("DEPT").build();
      RelNode semiRight = b.scan("BONUS").build();
      return b.push(left)
          .push(right)
          .join(JoinRelType.INNER,
              b.equals(b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .push(semiRight)
          .join(type,
              b.equals(b.field(2, 0, "JOB"),
                  b.field(2, 1, "JOB")))
          .build();
    };
    relFn(relFn).withRule(CoreRules.JOIN_TO_MULTI_JOIN).check();
  }

  @Test void testPushFilterPastAgg() {
    final String sql = "select dname, c from\n"
        + "(select name dname, count(*) as c from dept group by name) t\n"
        + " where dname = 'Charlie'";
    sql(sql).withRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6011">[CALCITE-6011]
   * Add the planner rule that pushes the Filter past a Window</a>. */
  @Test void testPushFilterPastWindowWithOnePartitionColumn() {
    final String sql = "select * from\n"
        + "(select NAME, DEPTNO, count(*) over (partition by DEPTNO) from dept)\n"
        + "where DEPTNO > 0";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .withRule(
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_WINDOW_TRANSPOSE,
            CoreRules.PROJECT_REMOVE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6011">[CALCITE-6011]
   * Add the planner rule that pushes the Filter past a Window</a>. */
  @Test void testPushFilterPastWindowWithTwoPartitionColumns() {
    final String sql = "select * from\n"
        + "(select NAME, DEPTNO, count(*) over (partition by NAME, DEPTNO) from dept)\n"
        + "where DEPTNO > 0";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .withRule(
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_WINDOW_TRANSPOSE,
            CoreRules.PROJECT_REMOVE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6011">[CALCITE-6011]
   * Add the planner rule that pushes the Filter past a Window</a>. */
  @Test void testPushFilterPastWindowWithDoubleWindows() {
    final String sql = "select * from\n"
        + "(select NAME, DEPTNO, count(*) over (partition by NAME, DEPTNO) as cnt,\n"
        + "sum(1) over (partition by DEPTNO) as all_sum from dept) t\n"
        + "where DEPTNO = 1";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .withRule(
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_WINDOW_TRANSPOSE,
            CoreRules.PROJECT_REMOVE).check();
  }

  private RelOptFixture basePushFilterPastAggWithGroupingSets() {
    return sql("${sql}")
        .withPreRule(CoreRules.PROJECT_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE)
        .withRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
  }

  @Test void testPushFilterPastAggWithGroupingSets1() {
    basePushFilterPastAggWithGroupingSets().checkUnchanged();
  }

  @Test void testPushFilterPastAggWithGroupingSets2() {
    basePushFilterPastAggWithGroupingSets().check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-434">[CALCITE-434]
   * FilterAggregateTransposeRule loses conditions that cannot be pushed</a>. */
  @Test void testPushFilterPastAggTwo() {
    final String sql = "select dept1.c1 from (\n"
        + "select dept.name as c1, count(*) as c2\n"
        + "from dept where dept.name > 'b' group by dept.name) dept1\n"
        + "where dept1.c1 > 'c' and (dept1.c2 > 30 or dept1.c1 < 'z')";
    sql(sql).withRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-799">[CALCITE-799]
   * Incorrect result for {@code HAVING count(*) > 1}</a>. */
  @Test void testPushFilterPastAggThree() {
    final String sql = "select deptno from emp\n"
        + "group by deptno having count(*) > 1";
    sql(sql).withRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1109">[CALCITE-1109]
   * FilterAggregateTransposeRule pushes down incorrect condition</a>. */
  @Test void testPushFilterPastAggFour() {
    final String sql = "select emp.deptno, count(*) from emp where emp.sal > '12'\n"
        + "group by emp.deptno";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_FILTER_TRANSPOSE)
        .withRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5425">[CALCITE-5425]
   * Should not pushdown Filter through Aggregate without group keys</a>. */
  @Test void testDoNotPushFilterThroughAggregateWithoutGroupKeys() {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode aggregate = b
          .scan("EMP")
          .aggregate(b.groupKey(), b.countStar(null))
          .build();

      // The reason why we use 'create' instead of 'RelBuilder#filter' is to prevent RelBuilder
      // optimizing in 'filter', which would transform the RelBuilder to 'empty()' when the
      // Filter is always false, this way, we won't be able to reproduce the problem
      // described in CALCITE-5425.
      return LogicalFilter.create(aggregate, b.getRexBuilder().makeLiteral(false));
    };
    relFn(relFn).withRule(FilterAggregateTransposeRule.Config.DEFAULT.toRule()).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-448">[CALCITE-448]
   * FilterIntoJoinRule creates filters containing invalid RexInputRef</a>. */
  @Test void testPushFilterPastProject() {
    final FilterJoinRule.Predicate predicate =
        (join, joinType, exp) -> joinType != JoinRelType.INNER;
    final FilterJoinRule.JoinConditionPushRule join =
        CoreRules.JOIN_CONDITION_PUSH.config
            .withPredicate(predicate)
            .withDescription("FilterJoinRule:no-filter")
            .as(FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig.class)
            .toRule();
    final FilterJoinRule.FilterIntoJoinRule filterOnJoin =
        CoreRules.FILTER_INTO_JOIN.config
            .withSmart(true)
            .withPredicate(predicate)
            .as(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.class)
            .toRule();
    final HepProgram program =
        HepProgram.builder()
            .addGroupBegin()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(join)
            .addRuleInstance(filterOnJoin)
            .addGroupEnd()
            .build();
    final String sql = "select a.name\n"
        + "from dept a\n"
        + "left join dept b on b.deptno > 10\n"
        + "right join dept c on b.deptno > 10\n";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_MERGE)
        .withProgram(program)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4499">[CALCITE-4499]
   * FilterJoinRule misses opportunity to push filter to semijoin input</a>. */
  @Test void testPushFilterSemijoin() {
    final FilterJoinRule.Predicate predicate =
        (join, joinType, exp) -> joinType != JoinRelType.INNER;
    final FilterJoinRule.JoinConditionPushRule join =
        CoreRules.JOIN_CONDITION_PUSH.config
            .withPredicate(predicate)
            .withDescription("FilterJoinRule:no-filter")
            .as(FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig.class)
            .toRule();

    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode left = b.scan("DEPT").build();
      RelNode right = b.scan("EMP").build();
      return b.push(left)
          .push(right)
          .semiJoin(
              b.and(
                  b.equals(b.field(2, 0, 0),
                      b.field(2, 1, 7)),
                  b.equals(b.field(2, 1, 5),
                      b.literal(100))))
          .project(b.field(1))
          .build();
    };

    relFn(relFn).withRule(join).check();
  }

  @Test void testSemiJoinProjectTranspose() {
    // build a rel equivalent to sql:
    // select a.name from dept a
    // where a.deptno in (select b.deptno * 2 from dept);
    checkSemiOrAntiJoinProjectTranspose(JoinRelType.SEMI);
  }

  @Test void testAntiJoinProjectTranspose() {
    // build a rel equivalent to sql:
    // select a.name from dept a
    // where a.deptno not in (select b.deptno * 2 from dept);
    checkSemiOrAntiJoinProjectTranspose(JoinRelType.ANTI);
  }

  private void checkSemiOrAntiJoinProjectTranspose(JoinRelType type) {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode left = b.scan("DEPT").build();
      RelNode right = b.scan("DEPT")
          .project(
              b.call(
                  SqlStdOperatorTable.MULTIPLY, b.literal(2), b.field(0)))
          .aggregate(b.groupKey(ImmutableBitSet.of(0))).build();

      return b.push(left)
          .push(right)
          .join(type,
              b.equals(b.field(2, 0, 0),
                  b.field(2, 1, 0)))
          .project(b.field(1))
          .build();
    };
    relFn(relFn).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testJoinProjectTranspose1() {
    final String sql = "select a.name\n"
        + "from dept a\n"
        + "left join dept b on b.deptno > 10\n"
        + "right join dept c on b.deptno > 10\n";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_JOIN_TRANSPOSE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER,
            CoreRules.PROJECT_MERGE,
            CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER,
            CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER,
            CoreRules.PROJECT_MERGE)
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
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
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
        .withRule(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER)
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
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
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
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
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
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
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
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
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
        .withRule(CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER)
        .check();
  }

  @Test void testJoinProjectTransposeWindow() {
    final String sql = "select *\n"
        + "from dept a\n"
        + "join (select rank() over (order by name) as r, 1 + 1 from dept) as b\n"
        + "on a.name = b.r";
    sql(sql)
        .withRule(CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-889">[CALCITE-889]
   * Implement SortUnionTransposeRule</a>. */
  @Test void testSortUnionTranspose() {
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 10";
    sql(sql)
        .withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.SORT_UNION_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-889">[CALCITE-889]
   * Implement SortUnionTransposeRule</a>. */
  @Test void testSortUnionTranspose2() {
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name";
    sql(sql)
        .withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-987">[CALCITE-987]
   * Push limit 0 will result in an infinite loop</a>. */
  @Test void testSortUnionTranspose3() {
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 0";
    sql(sql)
        .withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH)
        .check();
  }

  @Test void testSortRemovalAllKeysConstant() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal\n"
        + "order by deptno desc nulls last";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_CONSTANT_KEYS)
        .check();
  }

  @Test void testSortRemovalOneKeyConstant() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal\n"
        + "order by deptno, sal desc nulls first";
    sql(sql)
        // Use VolcanoPlanner with collation to ensure the correct creation of the new Sort
        .withVolcanoPlanner(false,  p -> {
          p.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(p, false, false);
        })
        .withRule(CoreRules.SORT_REMOVE_CONSTANT_KEYS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6507">[CALCITE-6507]
   * Random functions are incorrectly considered deterministic</a>. */
  @Test void testSortRemoveConstantKeyDoesNotRemoveOrderByRand() {
    final String sql = "SELECT ename FROM emp ORDER BY RAND()";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_CONSTANT_KEYS)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6507">[CALCITE-6507]
   * Random functions are incorrectly considered deterministic</a>. */
  @Test void testSortRemoveConstantKeyDoesNotRemoveOrderByRandInteger() {
    final String sql = "SELECT ename FROM emp ORDER BY RAND_INTEGER(2)";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_CONSTANT_KEYS)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6507">[CALCITE-6507]
   * Random functions are incorrectly considered deterministic</a>. */
  @Test void testSortRemoveConstantKeyDoesNotRemoveOrderByRandom() {
    final String sql = "SELECT ename FROM emp ORDER BY RANDOM()";
    sql(sql)
        .withFactory(f ->
            f.withOperatorTable(opTab ->
                SqlValidatorTest.operatorTableFor(SqlLibrary.POSTGRESQL)))
        .withRule(CoreRules.SORT_REMOVE_CONSTANT_KEYS)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-873">[CALCITE-873]
   * SortRemoveConstantKeysRule should remove NULL literal sort keys
   * (e.g. ORDER BY NULL)</a>. */
  @Test void testSortRemoveConstantKeyWhenOrderByIsNull() {
    final String sql = "SELECT * FROM emp ORDER BY deptno, null, empno";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_CONSTANT_KEYS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5994">[CALCITE-5994]
   * Add optimization rule to remove Sort when its input's row number
   * is less or equal to one</a>. */
  @Test void testSortRemoveWhenAggregateMaxRowCntIsOne() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp order by c";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5994">[CALCITE-5994]
   * Add optimization rule to remove Sort when its input's row number
   * is less or equal to one</a>. */
  @Test void testSortRemoveWhenLimitMaxRowCntIsOne() {
    final String sql = "select *\n"
        + "from (select * from sales.emp limit 1)\n"
        + "order by deptno";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT,
            CoreRules.SORT_PROJECT_TRANSPOSE,
            CoreRules.PROJECT_REMOVE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6009">[CALCITE-6009]
   * Add optimization to remove redundant LIMIT that is more than
   * input row count</a>. */
  @Test void testSortRemoveWhenInputValuesMaxRowCntLessOrEqualLimitFetch() {
    final String sql = "select * from\n"
        + "(VALUES 1,2,3,4,5,6) as t1 limit 10";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6009">[CALCITE-6009]
   * Add optimization to remove redundant LIMIT that is more than input
   * row count</a>. */
  @Test void testSortRemoveWhenInputAggregateMaxRowCntLessOrEqualLimitFetch() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp limit 20";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6009">[CALCITE-6009]
   * Add optimization to remove redundant LIMIT that is more than input
   * row count</a>. */
  @Test void testSortRemoveWhenHasOffset() {
    final String sql = "select * from\n"
        + "(select * from sales.emp limit 10) t limit 20 offset 1";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6038">[CALCITE-6038]
   * Remove 'ORDER BY ... LIMIT n' when input has at most one row, n >= 1,
   * and there is no 'OFFSET' clause</a>. */
  @Test void testSortRemoveWhenIsOrderAndLimit() {
    final String sql = "SELECT count(*) FROM sales.emp ORDER BY 1 LIMIT 10";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6038">[CALCITE-6038]
   * Remove 'ORDER BY ... LIMIT n' when input has at most one row, n >= 1,
   * and there is no 'OFFSET' clause</a>. */
  @Test void testSortNotRemoveWhenIsOrderAndLimit() {
    final String sql = "select * from\n"
        + "(SELECT * FROM sales.emp limit 100)\n"
        + "ORDER BY 1 LIMIT 10";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6038">[CALCITE-6038]
   * Remove 'ORDER BY ... LIMIT n' when input has at most one row, n >= 1,
   * and there is no 'OFFSET' clause</a>. */
  @Test void testSortNotRemoveWhenLimitFetchIsZeroHasOrder() {
    final String sql = "select * from\n"
        + "(SELECT * FROM sales.emp limit 1)\n"
        + "ORDER BY 1 LIMIT 0";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6038">[CALCITE-6038]
   * Remove 'ORDER BY ... LIMIT n' when input has at most one row, n >= 1,
   * and there is no 'OFFSET' clause</a>. */
  @Test void testSortNotRemoveWhenLimitFetchIsZeroWithoutOrder() {
    final String sql = "SELECT count(*) FROM sales.emp LIMIT 0";
    sql(sql)
        .withRule(CoreRules.SORT_REMOVE_REDUNDANT)
        .checkUnchanged();
  }

  /** Tests that an {@link EnumerableLimit} and {@link EnumerableSort} are
   * replaced by an {@link EnumerableLimitSort}, per
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3920">[CALCITE-3920]
   * Improve ORDER BY computation in Enumerable convention by exploiting
   * LIMIT</a>. */
  @Test void testLimitSort() {
    final String sql = "select mgr from sales.emp\n"
        + "union select mgr from sales.emp\n"
        + "order by mgr limit 10 offset 5";
    final RelOptFixture fixture = sql(sql)
        .withVolcanoPlanner(false)
        .withDecorrelate(true);
    RelNode rel = fixture.toRel();

    String planBefore = NL + RelOptUtil.toString(rel);
    final DiffRepository diffRepos = fixture.diffRepos;
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    RuleSet ruleSet =
        RuleSets.ofList(EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    Program program = Programs.of(ruleSet);

    RelTraitSet toTraits =
        rel.getCluster().traitSet()
            .replace(0, EnumerableConvention.INSTANCE);

    RelNode relAfter =
        program.run(fixture.planner, rel, toTraits,
            Collections.emptyList(), Collections.emptyList());

    String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5940">[CALCITE-5940]
   * Add the Rule to merge Limit</a>. */
  @Test void testLimitMerge() {
    final String sql = "select deptno from\n"
        + "(select deptno from emp where sal > 100 limit 10000)\n"
        + "limit 10";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.LIMIT_MERGE, CoreRules.PROJECT_REMOVE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5940">[CALCITE-5940]
   * Add the Rule to merge Limit</a>. */
  @Test void testLimitMerge1() {
    final String sql = "select deptno from\n"
        + "(select deptno from emp where sal > 100 order by sal desc limit 10000)\n"
        + "limit 10";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.LIMIT_MERGE, CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5940">[CALCITE-5940]
   * Add the Rule to merge Limit</a>. */
  @Test void testLimitMerge2() {
    final String sql = "select deptno from\n"
        + "(select deptno from emp where sal > 100 order by sal desc limit 10)\n"
        + "limit 10000";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.LIMIT_MERGE, CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5940">[CALCITE-5940]
   * Add the Rule to merge Limit</a>. */
  @Test void testLimitMerge3() {
    final String sql = "select deptno from\n"
        + "(select deptno from emp where sal > 100 limit 100)\n"
        + "order by deptno desc";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.LIMIT_MERGE, CoreRules.PROJECT_REMOVE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5940">[CALCITE-5940]
   * Add the Rule to merge Limit</a>. */
  @Test void testLimitMerge4() {
    final String sql = "select deptno from\n"
        + "(select deptno from emp where sal > 100 order by sal desc limit 10)\n"
        + "order by deptno limit 10000";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.LIMIT_MERGE, CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5940">[CALCITE-5940]
   * Add the Rule to merge Limit</a>. */
  @Test void testLimitMerge5() {
    final String sql = "select deptno from\n"
        + "(select deptno from emp where sal > 100 order by sal desc limit 10000)\n"
        + "order by deptno limit 10";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.LIMIT_MERGE, CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5940">[CALCITE-5940]
   * Add the Rule to merge Limit</a>. */
  @Test void testLimitMerge6() {
    final String sql = "select deptno from\n"
        + "(select deptno from emp where sal > 100 offset 100)\n"
        + "limit 10";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.LIMIT_MERGE)
        .checkUnchanged();
  }

  @Test void testSemiJoinRuleExists() {
    final String sql = "select * from dept where exists (\n"
        + "  select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.sal > 100)";
    sql(sql)
        .withExpand(true)
        .withDecorrelate(true)
        .withTrim(true)
        .withRelBuilderConfig(b -> b.withPruneInputOfAggregate(true))
        .withPreRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .check();
  }

  @Test void testIncorrectInType() {
    final String sql = "select ename from emp "
        + "  where ename in ( 'Sebastian' ) and ename = 'Sebastian' and deptno < 100";
    sql(sql)
        .withTrim(true)
        .withRule()
        .checkUnchanged();
  }

  @Test void testSemiJoinRule() {
    final String sql = "select dept.* from dept join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withDecorrelate(true)
        .withTrim(true)
        .withPreRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .check();
  }

  @Test void testSemiJoinRuleDoNotMatchAggregate() {
    final String sql = "select *\n"
        + "from emp\n"
        + "where exists(select * from dept where emp.deptno = dept.deptno)";
    sql(sql)
        .withExpand(true)
        .withDecorrelate(true)
        .withPreRule(CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN)
        .checkUnchanged();
  }

  @Test void testSemiJoinRuleWithJoinOnUniqueInput() {
    final String sql = "select *\n"
        + "from emp\n"
        + "where exists(select * from dept where emp.deptno = dept.deptno)";
    sql(sql)
        .withExpand(true)
        .withDecorrelate(true)
        .withTrim(true)
        .withRule(CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN)
        .check();
  }

  /**
   * Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5391">[CALCITE-5391]
   * JoinOnUniqueToSemiJoinRule should preserve field names, if possible</a>. */
  @Test void testSemiJoinRuleWithJoinOnUniqueInputWithAlias() {
    final String sql = "select emp.deptno as department_id, emp.sal as salary\n"
        + "from emp\n"
        + "where exists(select * from dept where emp.deptno = dept.deptno)";
    sql(sql)
        .withExpand(true)
        .withDecorrelate(true)
        .withTrim(true)
        .withRule(CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1495">[CALCITE-1495]
   * SemiJoinRule should not apply to RIGHT and FULL JOIN</a>. */
  @Test void testSemiJoinRuleRight() {
    final String sql = "select dept.* from dept right join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPreRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .withDecorrelate(true)
        .withTrim(true)
        .checkUnchanged();
  }

  /** Similar to {@link #testSemiJoinRuleRight()} but FULL. */
  @Test void testSemiJoinRuleFull() {
    final String sql = "select dept.* from dept full join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPreRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .withDecorrelate(true)
        .withTrim(true)
        .checkUnchanged();
  }

  /** Similar to {@link #testSemiJoinRule()} but LEFT. */
  @Test void testSemiJoinRuleLeft() {
    final String sql = "select name from dept left join (\n"
        + "  select distinct deptno from emp\n"
        + "  where sal > 100) using (deptno)";
    sql(sql)
        .withPreRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .withDecorrelate(true)
        .withTrim(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4941">[CALCITE-4941]
   * SemiJoinRule loses hints</a>. */
  @Test void testSemiJoinRuleWithHint() {
    final RelHint noHashJoinHint = RelHint.builder("no_hash_join").build();
    final Function<RelBuilder, RelNode> relFn = b -> {
      b.getCluster().setHintStrategies(
          HintStrategyTable.builder()
              .hintStrategy("no_hash_join", HintPredicates.JOIN)
              .build());
      return b
          .scan("DEPT")
          .scan("EMP")
          .project(b.field("DEPTNO"))
          .distinct()
          .join(
              JoinRelType.INNER,
              b.equals(
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO"))).hints(noHashJoinHint)
          .project(b.field("DNAME"))
          .build();
    };

    // verify plan
    relFn(relFn)
        .withRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .check();

    // verify hint
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    final RelNode input = relFn.apply(relBuilder);
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(input);
    final RelNode output = hepPlanner.findBestExp();
    final Join join = (Join) output.getInput(0);
    assertTrue(join.getHints().contains(noHashJoinHint));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-438">[CALCITE-438]
   * Push predicates through SemiJoin</a>. */
  @Test void testPushFilterThroughSemiJoin() {
    final String sql = "select * from (\n"
        + "  select * from dept where dept.deptno in (\n"
        + "    select emp.deptno from emp))R\n"
        + "where R.deptno <=10";
    sql(sql)
        .withExpand(true)
        .withDecorrelate(true)
        .withTrim(false)
        .withPreRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-571">[CALCITE-571]
   * ReduceExpressionsRule tries to reduce SemiJoin condition to non-equi
   * condition</a>. */
  @Test void testSemiJoinReduceConstants() {
    final String sql = "select e1.sal\n"
        + "from (select * from emp where deptno = 200) as e1\n"
        + "where e1.deptno in (\n"
        + "  select e2.deptno from emp e2 where e2.sal = 100)";
    sql(sql)
        .withDecorrelate(false)
        .withExpand(true)
        .withTrim(true)
        .withPreRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .withRule(CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testSemiJoinTrim() {
    final String sql = "select s.deptno\n"
        + "from (select *\n"
        + "  from dept\n"
        + "  where exists (\n"
        + "    select * from emp\n"
        + "    where emp.deptno = dept.deptno\n"
        + "    and emp.sal > 100)) s\n"
        + "join customer.account on s.deptno = account.acctno";

    final HepProgram program =
        HepProgram.builder()
            .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.PROJECT_MERGE)
            .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
            .build();

    sql(sql)
        .withExpand(true)
        .withDecorrelate(true)
        .withPre(program)
        .withRule() // empty program
        .withAfter((fixture, r) ->
            fixture.tester.trimRelNode(
                fixture.factory.withSqlToRelConfig(c ->
                    c.withTrimUnusedFields(true)), r))
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5169">[CALCITE-5169]
   * 'xx < 1 OR xx > 1' cannot be simplified to 'xx <> 1'</a>.
   *
   * <p>{@code ename <> '' and ename <> '3'} should be simplified to Sarg.
   */
  @Test void testExpressionSimplification1() {
    final String sql = "select * from emp\n"
        + "where ename <> '' and ename <> '3'";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5169">[CALCITE-5169]
   * 'xx < 1 OR xx > 1' cannot be simplified to 'xx <> 1'</a>.
   *
   * <p>{@code (ename < '' or ename > '') and (ename < '3' or ename > '3')} should be
   * simplified to Sarg too.
   * The difference between this and {@link #testExpressionSimplification1()} is
   * that '<' and '<>' have different
   * {@link org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency} which
   * will lead to different type inference result for literals. These two tests
   * show that the simplification could handle this case.
   */
  @Test void testExpressionSimplification2() {
    final String sql = "select * from emp\n"
        + "where (ename < '' or ename > '') and (ename < '3' or ename > '3')";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceAverage() {
    final String sql = "select name, max(name), avg(deptno), min(name)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1621">[CALCITE-1621]
   * Adding a cast around the null literal in aggregate rules</a>. */
  @Test void testCastInAggregateReduceFunctions() {
    final String sql = "select name, stddev_pop(deptno), avg(deptno),\n"
        + "stddev_samp(deptno),var_pop(deptno), var_samp(deptno)\n"
        + "from sales.dept group by name";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .check();
  }

  @Test void testDistinctCountWithoutGroupBy() {
    final String sql = "select max(deptno), count(distinct ename)\n"
        + "from sales.emp";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.AGGREGATE_PROJECT_MERGE)
        .check();
  }

  @Test void testDistinctCount1() {
    final String sql = "select deptno, count(distinct ename)\n"
        + "from sales.emp group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.AGGREGATE_PROJECT_MERGE)
        .check();
  }

  @Test void testDistinctCount2() {
    final String sql = "select deptno, count(distinct ename), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.AGGREGATE_PROJECT_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1293">[CALCITE-1293]
   * Bad code generated when argument to COUNT(DISTINCT) is a # GROUP BY
   * column</a>. */
  @Test void testDistinctCount3() {
    final String sql = "select count(distinct deptno), sum(sal)"
        + " from sales.emp group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .check();
  }

  /** Tests implementing multiple distinct count the old way, using a join. */
  @Test void testDistinctCountMultipleViaJoin() {
    final String sql = "select deptno, count(distinct ename),\n"
        + "  count(distinct job, ename),\n"
        + "  count(distinct deptno, job), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
            CoreRules.AGGREGATE_PROJECT_MERGE)
        .check();
  }

  /** Tests implementing multiple distinct count the new way, using GROUPING
   * SETS. */
  @Test void testDistinctCountMultiple() {
    final String sql = "select deptno, count(distinct ename),\n"
        + "  count(distinct job)\n"
        + "from sales.emp group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.AGGREGATE_PROJECT_MERGE)
        .check();
  }

  @Test void testDistinctCountMultipleNoGroup() {
    final String sql = "select count(distinct ename), count(distinct job)\n"
        + "from sales.emp";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.AGGREGATE_PROJECT_MERGE)
        .check();
  }

  @Test void testDistinctCountMixedJoin() {
    final String sql = "select deptno, count(distinct ename), count(distinct job, ename),\n"
        + "count(distinct deptno, job), sum(sal)\n"
        + "from sales.emp group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
            CoreRules.AGGREGATE_PROJECT_MERGE)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6332">
   * [CALCITE-6332] Optimization CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN
   * produces incorrect results for aggregates with groupSets</a>. */
  @Test void testIssue6332() {
    final String sql = "select count(distinct deptno) as cd, count(*) as c\n"
        + "from emp\n"
        + "group by cube(deptno)";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .checkUnchanged();
  }

  /** Test case for <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6349">
   * [CALCITE-6349] CoreRules.PROJECT_REDUCE_EXPRESSIONS crashes on expression
   * with ARRAY_REPEAT</a>. */
  @Test void testArrayRepeat() {
    final String sql = "select array_repeat(1, null)";
    sql(sql)
        .withFactory(
            t -> t.withOperatorTable(
                opTab -> SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.STANDARD, SqlLibrary.SPARK)))
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testDistinctCountMixed() {
    final String sql = "select deptno, count(distinct deptno, job) as cddj,\n"
        + "  sum(sal) as s\n"
        + "from sales.emp group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testDistinctCountMixed2() {
    final String sql = "select deptno, count(distinct ename) as cde,\n"
        + "count(distinct job, ename) as cdje,\n"
        + "count(distinct deptno, job) as cddj,\n"
        + "sum(sal) as s\n"
        + "from sales.emp group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testDistinctCountGroupingSets1() {
    final String sql = "select deptno, job, count(distinct ename)\n"
        + "from sales.emp group by rollup(deptno,job)";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6353">[CALCITE-6353]
   * Optimization CoreRules.PROJECT_REDUCE_EXPRESSIONS crashes
   * while optimizing ARRAY_CONCAT expression</a>. */
  @Test void testArrayConcat() {
    final String sql = "select array_concat(ARRAY [1, 2], ARRAY [3, 4])";
    sql(sql).withFactory(
        t -> t.withOperatorTable(
            opTab -> SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                SqlLibrary.STANDARD, SqlLibrary.BIG_QUERY)))
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testDistinctCountGroupingSets2() {
    final String sql = "select deptno, job, count(distinct ename), sum(sal)\n"
        + "from sales.emp group by rollup(deptno,job)";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testDistinctNonDistinctAggregates() {
    final String sql = "select emp.empno, count(*), avg(distinct dept.deptno)\n"
        + "from sales.emp emp inner join sales.dept dept\n"
        + "on emp.deptno = dept.deptno\n"
        + "group by emp.empno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
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
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5884">[CALCITE-5884]
   * ARRAY_TO_STRING function should return NULL if its 'nullValue' argument is NULL</a>. */
  @Test void testArrayToString() {
    final String sql = "select array_to_string(array['1','2','3','4',NULL,'6'], ',', NULL)";
    // We expect the result to be NULL, since array_to_string returns NULL if
    // any argument is NULL.
    sql(sql).withFactory(
        t -> t.withOperatorTable(
            opTab -> SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                SqlLibrary.STANDARD, SqlLibrary.BIG_QUERY))) // for array_to_string function
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
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
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testDistinctNonDistinctAggregatesWithGrouping2() {
    final String sql = "SELECT deptno, COUNT(deptno), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testDistinctNonDistinctTwoAggregatesWithGrouping() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testDistinctWithGrouping() {
    final String sql = "SELECT sal, SUM(comm), MIN(comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY sal";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testRemoveDistinctOnAgg() {
    final String sql = "SELECT empno, SUM(distinct sal), MIN(sal), "
        + "MIN(distinct sal), MAX(distinct sal), "
        + "bit_and(distinct sal), bit_or(sal), count(distinct sal) "
        + "from sales.emp group by empno, deptno\n";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_REMOVE,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testMultipleDistinctWithGrouping() {
    final String sql = "SELECT sal, SUM(comm), AVG(DISTINCT comm), SUM(DISTINCT sal)\n"
        + "FROM emp\n"
        + "GROUP BY sal";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testDistinctWithMultipleInputs() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), COUNT(DISTINCT sal, comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testDistinctWithMultipleInputsAndGroupby() {
    final String sql = "SELECT deptno, SUM(comm), MIN(comm), COUNT(DISTINCT sal, deptno, comm)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testDistinctWithFilterWithoutGroupByUsingJoin() {
    final String sql = "SELECT SUM(comm), COUNT(DISTINCT sal) FILTER (WHERE sal > 1000)\n"
                       + "FROM emp";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testMultipleDistinctWithSameArgsDifferentFilterUsingJoin() {
    final String sql = "select deptno, "
                       + "count(distinct sal) FILTER (WHERE sal > 1000), "
                       + "count(distinct sal) FILTER (WHERE sal > 500) "
                       + "from sales.emp group by deptno";
    sql(sql)
        .withRule(
            CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,
            CoreRules.AGGREGATE_PROJECT_MERGE
        )
        .check();
  }

  @Test void testDistinctWithFilterWithoutGroupBy() {
    final String sql = "SELECT SUM(comm), COUNT(DISTINCT sal) FILTER (WHERE sal > 1000)\n"
        + "FROM emp";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .check();
  }

  @Test void testDistinctWithDiffFiltersAndSameGroupSet() {
    final String sql = "SELECT COUNT(DISTINCT c) FILTER (WHERE d),\n"
        + "COUNT(DISTINCT d) FILTER (WHERE c)\n"
        + "FROM (select sal > 1000 is true as c, sal < 500 is true as d, comm from emp)";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5985">[CALCITE-5985]
   * FilterTableFunctionTransposeRule should not use "Logical" RelNodes</a>. */
  @Test void testFilterTableFunctionScanTranspose() {
    // "tfrt" table function is the one from MockSqlOperatorTable,
    // that implement TableFunctionReturnTypeInference.
    final String sql = "select * from table(tfrt(cursor(select name from dept)))"
        + " where name = '1'";

    sql(sql)
        .withRule(CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE)
        .check();
  }

  @Test void testDistinctWithFilterAndGroupBy() {
    final String sql = "SELECT deptno, SUM(comm), COUNT(DISTINCT sal) FILTER (WHERE sal > 1000)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES)
        .check();
  }

  /** Tests {@link AggregateExpandWithinDistinctRule}. The generated query
   * throws if arguments are not functionally dependent on the distinct key. */
  @Test void testWithinDistinct() {
    final String sql = "SELECT deptno, SUM(sal), SUM(sal) WITHIN DISTINCT (job)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT)
        .build();
    sql(sql).withProgram(program).check();
  }

  /** As {@link #testWithinDistinct()}, but the generated query does not throw
   * if arguments are not functionally dependent on the distinct key.
   *
   * @see AggregateExpandWithinDistinctRule.Config#throwIfNotUnique() */
  @Test void testWithinDistinctNoThrow() {
    final String sql = "SELECT deptno, SUM(sal), SUM(sal) WITHIN DISTINCT (job)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT
            .config.withThrowIfNotUnique(false).toRule())
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Tests {@link AggregateExpandWithinDistinctRule}. If all aggregate calls
   * have the same distinct keys, there is no need for multiple grouping
   * sets. */
  @Test void testWithinDistinctUniformDistinctKeys() {
    final String sql = "SELECT deptno,\n"
        + " SUM(sal) WITHIN DISTINCT (job),\n"
        + " AVG(comm) WITHIN DISTINCT (job)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT)
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Tests {@link AggregateExpandWithinDistinctRule}. If all aggregate calls
   * have the same distinct keys, and we're not checking for true uniqueness,
   * there is no need for filtering in the outer aggregate. */
  @Test void testWithinDistinctUniformDistinctKeysNoThrow() {
    final String sql = "SELECT deptno,\n"
        + " SUM(sal) WITHIN DISTINCT (job),\n"
        + " AVG(comm) WITHIN DISTINCT (job)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(
            CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT.config
                .withThrowIfNotUnique(false).toRule())
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Tests that {@link AggregateExpandWithinDistinctRule} treats
   * "COUNT(DISTINCT x)" as if it were "COUNT(x) WITHIN DISTINCT (x)". */
  @Test void testWithinDistinctCountDistinct() {
    final String sql = "SELECT deptno,\n"
        + "  SUM(sal) WITHIN DISTINCT (comm) AS ss_c,\n"
        + "  COUNT(DISTINCT job) cdj,\n"
        + "  COUNT(job) WITHIN DISTINCT (job) AS cj_j,\n"
        + "  COUNT(DISTINCT job) WITHIN DISTINCT (job) AS cdj_j,\n"
        + "  COUNT(DISTINCT job) FILTER (WHERE sal > 1000) AS cdj_filtered\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT
            .config.withThrowIfNotUnique(false).toRule())
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4726">[CALCITE-4726]
   * Support aggregate calls with a FILTER clause in
   * AggregateExpandWithinDistinctRule</a>.
   *
   * <p>Tests {@link AggregateExpandWithinDistinctRule} with different
   * distinct keys and different filters for each aggregate call. */
  @Test void testWithinDistinctFilteredAggs() {
    final String sql = "SELECT deptno,\n"
        + " SUM(sal) WITHIN DISTINCT (job) FILTER (WHERE comm > 10),\n"
        + " AVG(comm) WITHIN DISTINCT (sal) FILTER (WHERE ename LIKE '%ok%')\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT)
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5846">[CALCITE-5846]
   * Preserve FILTER clause for non-distinct aggregate calls with
   * AggregateExpandWithinDistinctRule</a>.
   *
   * <p>Tests {@link AggregateExpandWithinDistinctRule} with a non-distinct aggregate with a FILTER
   * clause and a distinct aggregate in the same query. */
  @Test void testWithinDistinctPreservesNonDistinctAggFilters() {
    final String sql = "SELECT deptno,\n"
        + " SUM(sal) FILTER (WHERE sal > 1000),\n"
        + " SUM(sal) WITHIN DISTINCT (job)\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT)
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6595">[CALCITE-6595]
   * Preserve collation for non-distinct aggregate calls with AggregateExpandWithinDistinctRule</a>.
   *
   * <p>Test that AggregateExpandWithinDistinctRule preserves collation on non-distinct aggregates
   * with a WITHIN GROUP clause in a query that also includes a distinct aggregate. */
  @Test void testWithinDistinctPreservesNonDistinctCollation() {
    final String sql = "SELECT SUM(sal) WITHIN DISTINCT (job),\n"
        + "LISTAGG(ename, '; ') WITHIN GROUP (ORDER BY sal DESC)\n"
        + " FROM Emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT)
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Tests {@link AggregateExpandWithinDistinctRule}. Includes multiple
   * different filters for the aggregate calls, and all aggregate calls have the
   * same distinct keys, so there is no need to filter based on
   * {@code GROUPING()}. */
  @Test void testWithinDistinctFilteredAggsUniformDistinctKeys() {
    final String sql = "SELECT deptno,\n"
        + " SUM(sal) WITHIN DISTINCT (job) FILTER (WHERE comm > 10),\n"
        + " AVG(comm) WITHIN DISTINCT (job) FILTER (WHERE ename LIKE '%ok%')\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT)
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Tests {@link AggregateExpandWithinDistinctRule}. Includes multiple
   * different filters for the aggregate calls, and all aggregate calls have the
   * same distinct keys, so there is no need to filter based on
   * {@code GROUPING()}. Does <em>not</em> throw if not unique. */
  @Test void testWithinDistinctFilteredAggsUniformDistinctKeysNoThrow() {
    final String sql = "SELECT deptno,\n"
        + " SUM(sal) WITHIN DISTINCT (job) FILTER (WHERE comm > 10),\n"
        + " AVG(comm) WITHIN DISTINCT (job) FILTER (WHERE ename LIKE '%ok%')\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(
            CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT.config
                .withThrowIfNotUnique(false).toRule())
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Tests {@link AggregateExpandWithinDistinctRule}. Includes multiple
   * identical filters for the aggregate calls. The filters should be
   * re-used. */
  @Test void testWithinDistinctFilteredAggsSameFilter() {
    final String sql = "SELECT deptno,\n"
        + " SUM(sal) WITHIN DISTINCT (job) FILTER (WHERE ename LIKE '%ok%'),\n"
        + " AVG(comm) WITHIN DISTINCT (sal) FILTER (WHERE ename LIKE '%ok%')\n"
        + "FROM emp\n"
        + "GROUP BY deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT)
        .build();
    sql(sql).withProgram(program).check();
  }

  @Test void testPushProjectPastFilter() {
    final String sql = "select empno + deptno from emp where sal = 10 * comm\n"
        + "and upper(ename) = 'FOO'";
    sql(sql).withRule(CoreRules.PROJECT_FILTER_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1778">[CALCITE-1778]
   * Query with "WHERE CASE" throws AssertionError "Cast for just nullability
   * not allowed"</a>. */
  @Test void testPushProjectPastFilter2() {
    final String sql = "select count(*)\n"
        + "from emp\n"
        + "where case when mgr < 10 then true else false end";
    sql(sql).withRule(CoreRules.PROJECT_FILTER_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3975">[CALCITE-3975]
   * ProjectFilterTransposeRule should succeed for project that happens to
   * reference all input columns</a>. */
  @Test void testPushProjectPastFilter3() {
    checkPushProjectPastFilter3(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .checkUnchanged();
  }

  /** As {@link #testPushProjectPastFilter3()} but pushes down project and
   * filter expressions whole. */
  @Test void testPushProjectPastFilter3b() {
    checkPushProjectPastFilter3(CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS)
        .check();
  }

  /** As {@link #testPushProjectPastFilter3()} but pushes down project
   * expressions whole. */
  @Test void testPushProjectPastFilter3c() {
    checkPushProjectPastFilter3(
        CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS)
        .check();
  }

  RelOptFixture checkPushProjectPastFilter3(ProjectFilterTransposeRule rule) {
    final String sql = "select empno + deptno as x, ename, job, mgr,\n"
        + "  hiredate, sal, comm, slacker\n"
        + "from emp\n"
        + "where sal = 10 * comm\n"
        + "and upper(ename) = 'FOO'";
    return sql(sql).withRule(rule);
  }

  @Test void testPushProjectPastJoin() {
    final String sql = "select e.sal + b.comm from emp e inner join bonus b\n"
        + "on e.ename = b.ename and e.deptno = 10";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3004">[CALCITE-3004]
   * Should not push over past union but its operands can since setop
   * will affect row count</a>. */
  @Test void testProjectSetOpTranspose() {
    final String sql = "select job, sum(sal + 100) over (partition by deptno) from\n"
        + "(select * from emp e1 union all select * from emp e2)";
    sql(sql).withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE).check();
  }

  @Test void testProjectCorrelateTransposeDynamic() {
    ProjectCorrelateTransposeRule customPCTrans =
        ProjectCorrelateTransposeRule.Config.DEFAULT
            .withPreserveExprCondition(RelOptRulesTest::skipItem)
            .toRule();

    String sql = "select t1.c_nationkey, t2.a as fake_col2 "
        + "from SALES.CUSTOMER as t1, "
        + "unnest(t1.fake_col) as t2(a)";
    sql(sql)
        .withDynamicTable()
        .withRule(customPCTrans)
        .checkUnchanged();
  }

  @Test void testProjectCorrelateTransposeRuleLeftCorrelate() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1 "
        + "where exists (select empno, deptno from dept d2 where e1.deptno = d2.deptno)";
    sql(sql)
        .withDecorrelate(false)
        .withExpand(true)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.PROJECT_FILTER_TRANSPOSE,
            CoreRules.PROJECT_CORRELATE_TRANSPOSE)
        .check();
  }

  @Test void testProjectCorrelateTransposeRuleSemiCorrelate() {
    checkProjectCorrelateTransposeRuleSemiOrAntiCorrelate(JoinRelType.SEMI);
  }

  @Test void testProjectCorrelateTransposeRuleAntiCorrelate() {
    checkProjectCorrelateTransposeRuleSemiOrAntiCorrelate(JoinRelType.ANTI);
  }

  private void checkProjectCorrelateTransposeRuleSemiOrAntiCorrelate(JoinRelType type) {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode left = b
          .values(new String[]{"f", "f2"}, "1", "2").build();

      CorrelationId correlationId = new CorrelationId(0);
      RexNode rexCorrel =
          b.getRexBuilder().makeCorrel(
              left.getRowType(),
              correlationId);

      RelNode right = b
          .values(new String[]{"f3", "f4"}, "1", "2")
          .project(b.field(0),
              b.getRexBuilder().makeFieldAccess(rexCorrel, 0)).build();
      LogicalCorrelate correlate =
          new LogicalCorrelate(left.getCluster(), left.getTraitSet(),
              ImmutableList.of(), left, right, correlationId,
              ImmutableBitSet.of(0), type);

      b.push(correlate);
      return b.project(b.field(0))
          .build();
    };

    relFn(relFn).withRule(CoreRules.PROJECT_CORRELATE_TRANSPOSE).check();
  }

  @Test void testProjectCorrelateTransposeWithExprCond() {
    ProjectCorrelateTransposeRule customPCTrans =
        ProjectCorrelateTransposeRule.Config.DEFAULT
            .withPreserveExprCondition(RelOptRulesTest::skipItem)
            .toRule();

    final String sql = "select t1.name, t2.ename\n"
        + "from DEPT_NESTED as t1,\n"
        + "unnest(t1.employees) as t2";
    sql(sql).withRule(customPCTrans).check();
  }

  @Test void testSwapOuterJoinFieldAccess() {
    HepProgram preProgram = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(CoreRules.JOIN_COMMUTE_OUTER)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build();
    final String sql = "select t1.name, e.ename\n"
        + "from DEPT_NESTED as t1 left outer join sales.emp e\n"
        + " on t1.skill.type = e.job";
    sql(sql).withPre(preProgram).withProgram(program).check();
  }

  @Test void testProjectCorrelateTranspose() {
    ProjectCorrelateTransposeRule customPCTrans =
        ProjectCorrelateTransposeRule.Config.DEFAULT
            .withPreserveExprCondition(expr -> true)
            .toRule();
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
    sql(sql).withRule(CoreRules.PROJECT_CORRELATE_TRANSPOSE).check();
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
    sql(sql)
        .withDecorrelate(false)
        .withExpand(true)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE)
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
        CoreRules.FILTER_PROJECT_TRANSPOSE.config
            .withOperandSupplier(b0 ->
                b0.operand(Filter.class).predicate(filter -> true)
                    .oneInput(b1 ->
                        b1.operand(Project.class).predicate(project -> true)
                            .anyInputs()))
            .as(FilterProjectTransposeRule.Config.class)
            .withCopyFilter(true)
            .withCopyProject(true)
            .toRule();
    sql(sql)
        .withDecorrelate(false)
        .withExpand(true)
        .withRule(filterProjectTransposeRule)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6513">[CALCITE-6513]
   * FilterProjectTransposeRule may cause OOM when Project expressions are
   * complex</a>. */
  @Test void testFilterProjectTransposeBloat() {
    String sql =
        "SELECT x1 from\n"
        + "    (SELECT 'L1' || x0  || x0 || x0 || x0 as x1 from\n"
        + "        (SELECT 'L0' || ENAME || ENAME || ENAME || ENAME as x0 from emp) t1) t2\n"
        + "WHERE x1 = 'Something'";

    final FilterProjectTransposeRule filterProjectTransposeRule =
        CoreRules.FILTER_PROJECT_TRANSPOSE.config
            .withOperandSupplier(b0 ->
                b0.operand(Filter.class).predicate(filter -> true)
                    .oneInput(b1 ->
                        b1.operand(Project.class).predicate(project -> true)
                            .anyInputs()))
            .as(FilterProjectTransposeRule.Config.class)
            .withCopyFilter(true)
            .withCopyProject(true)
            .withBloat(3)
            .toRule();
    sql(sql)
        .withRelBuilderConfig(config -> config.withBloat(3))
        .withDecorrelate(false)
        .withExpand(true)
        .withRule(filterProjectTransposeRule)
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
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastInnerJoinStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from emp e inner join bonus b on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7065">[CALCITE-7065]
   * CoreRules.PROJECT_REDUCE_EXPRESSIONS crashes when applied to a lambda</a>. */
  @Test void testReduceLambda() {
    final String sql = "select \"EXISTS\"(ARRAY[1], x -> true)";
    sql(sql)
        .withFactory(f ->
            f.withOperatorTable(opTab ->
                SqlValidatorTest.operatorTableFor(SqlLibrary.SPARK)))
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS).checkUnchanged();
  }

  @Test void testPushProjectPastLeftJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e left outer join bonus b on e.ename = b.ename\n"
        + "group by case when e.sal < 11 then 11 else -1 * e.sal end";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastLeftJoinSwap() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from bonus b left outer join emp e on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastLeftJoinSwapStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from bonus b left outer join emp e on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastRightJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e right outer join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastRightJoinStrong() {
    final String sql = "select count(*),\n"
        + " case when e.sal < 11 then -1 * e.sal else e.sal end\n"
        + "from emp e right outer join bonus b on e.ename = b.ename\n"
        + "group by case when e.sal < 11 then -1 * e.sal else e.sal end";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastRightJoinSwap() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from bonus b right outer join emp e on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastRightJoinSwapStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from bonus b right outer join emp e on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastFullJoin() {
    final String sql = "select count(*), " + NOT_STRONG_EXPR + "\n"
        + "from emp e full outer join bonus b on e.ename = b.ename\n"
        + "group by " + NOT_STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastFullJoinStrong() {
    final String sql = "select count(*), " + STRONG_EXPR + "\n"
        + "from emp e full outer join bonus b on e.ename = b.ename\n"
        + "group by " + STRONG_EXPR;
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2343">[CALCITE-2343]
   * Should not push over whose columns are all from left child past join since
   * join will affect row count</a>. */
  @Test void testPushProjectWithOverPastJoin1() {
    final String sql = "select e.sal + b.comm,\n"
        + "count(e.empno) over (partition by e.deptno)\n"
        + "from emp e join bonus b\n"
        + "on e.ename = b.ename and e.deptno = 10";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  /** As {@link #testPushProjectWithOverPastJoin1()};
   * should not push over whose columns are all from right child past join since
   * join will affect row count. */
  @Test void testPushProjectWithOverPastJoin2() {
    final String sql = "select e.sal + b.comm,\n"
        + "count(b.sal) over (partition by b.job)\n"
        + "from emp e join bonus b\n"
        + "on e.ename = b.ename and e.deptno = 10";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  /** As {@link #testPushProjectWithOverPastJoin2()};
   * should not push over past join but should push the operands of over past
   * join. */
  @Test void testPushProjectWithOverPastJoin3() {
    final String sql = "select e.sal + b.comm,\n"
        + "sum(b.sal + b.sal + 100) over (partition by b.job)\n"
        + "from emp e join bonus b\n"
        + "on e.ename = b.ename and e.deptno = 10";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4982">[CALCITE-4982]
   * NonNull field shouldn't be pushed down into leaf of outer-join
   * in 'ProjectJoinTransposeRule'</a>. */
  @Test void testPushProjectPastOutJoinWithCastNonNullExpr() {
    final String sql = "select e.empno + 1 as c1, coalesce(d.name, b.job, '') as c2\n"
        + "from emp e\n"
        + "left join bonus b on e.ename = b.ename\n"
        + "left join dept d on e.deptno = d.deptno";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_TRANSPOSE).check();
  }

  @Test void testPushProjectPastSetOp() {
    final String sql = "select sal from\n"
        + "(select * from emp e1 union all select * from emp e2)";
    sql(sql).withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE).check();
  }

  @Test void testPushJoinThroughUnionOnLeft() {
    final String sql = "select r1.sal from\n"
        + "(select * from emp e1 union all select * from emp e2) r1,\n"
        + "emp r2";
    sql(sql).withRule(CoreRules.JOIN_LEFT_UNION_TRANSPOSE).check();
  }

  @Test void testPushJoinThroughUnionOnRight() {
    final String sql = "select r1.sal from\n"
        + "emp r1,\n"
        + "(select * from emp e1 union all select * from emp e2) r2";
    sql(sql).withRule(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE).check();
  }

  @Test void testPushJoinThroughUnionOnRightDoesNotMatchSemiJoin() {
    // build a rel equivalent to sql:
    // select r1.sal from
    // emp r1 where r1.deptno in
    //  (select deptno from dept d1 where deptno < 10
    //  union all
    //  select deptno from dept d2 where deptno > 20)
    checkPushJoinThroughUnionOnRightDoesNotMatchSemiOrAntiJoin(JoinRelType.SEMI);
  }

  @Test void testPushJoinThroughUnionOnRightDoesNotMatchAntiJoin() {
    // build a rel equivalent to sql:
    // select r1.sal from
    // emp r1 where r1.deptno not in
    //  (select deptno from dept d1 where deptno < 10
    //  union all
    //  select deptno from dept d2 where deptno > 20)
    checkPushJoinThroughUnionOnRightDoesNotMatchSemiOrAntiJoin(JoinRelType.ANTI);
  }

  private void checkPushJoinThroughUnionOnRightDoesNotMatchSemiOrAntiJoin(JoinRelType type) {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode left = b.scan("EMP").build();
      RelNode right = b
          .scan("DEPT")
          .filter(b.lessThan(b.field("DEPTNO"), b.literal(10)))
          .project(b.field("DEPTNO"))
          .scan("DEPT")
          .filter(b.greaterThan(b.field("DEPTNO"), b.literal(20)))
          .project(b.field("DEPTNO"))
          .union(true)
          .build();
      return b.push(left).push(right)
          .join(type,
              b.equals(b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .project(b.field("SAL"))
          .build();
    };
    relFn(relFn).withRule(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE).checkUnchanged();
  }

  @Test void testMergeFilterWithJoinCondition() {
    final String sql = "select d.name as dname,e.ename as ename\n"
        + " from emp e inner join dept d\n"
        + " on e.deptno=d.deptno\n"
        + " where d.name='Propane'";
    sql(sql)
        .withRule(CoreRules.JOIN_EXTRACT_FILTER,
            CoreRules.FILTER_TO_CALC,
            CoreRules.PROJECT_TO_CALC,
            CoreRules.CALC_MERGE)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5073">[CALCITE-5073]
   * JoinConditionPushRule cannot infer 'LHS.C1 = LHS.C2' from
   * 'LHS.C1 = RHS.C1 AND LHS.C2 = RHS.C1'</a>.
   */
  @Test void testJoinConditionPushdown1() {
    final String sql = "select *\n"
        + "from emp e1, emp e2, dept d2\n"
        + "where e1.deptno = d2.deptno and e2.deptno = d2.deptno";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.PROJECT_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5073">[CALCITE-5073]
   * JoinConditionPushRule cannot infer 'LHS.C1 = LHS.C2' from
   * 'LHS.C1 = RHS.C1 AND LHS.C2 = RHS.C1'</a>.
   */
  @Test void testJoinConditionPushdown2() {
    final String sql = "select *\n"
        + "from emp e, dept d\n"
        + "where e.deptno = d.deptno and e.empno = d.deptno";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.PROJECT_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE)
        .check();
  }

  /** Similar to {@link #testJoinConditionPushdown1()} but full join
   * from which more equality conditions can not be inferred. */
  @Test void testJoinConditionPushdown3() {
    final String sql = "select *\n"
        + "from emp e full join dept d\n"
        + "on e.deptno = d.deptno and e.empno = d.deptno";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.PROJECT_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE)
        .checkUnchanged();
  }

  /** Similar to {@link #testJoinConditionPushdown1()} but semi join
   * from which more equality conditions can be inferred. */
  @Test void testJoinConditionPushdown4() {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode left = b.scan("EMP")
          .project(
              b.field("DEPTNO"),
              b.field("ENAME"))
          .build();
      RelNode right = b.scan("DEPT")
          .project(
              b.field("DEPTNO"),
              b.field("DNAME"))
          .build();

      b.push(left).push(right);

      RexInputRef ref1 = b.field(2, 0, "DEPTNO");
      RexInputRef ref2 = b.field(2, 1, "DEPTNO");
      RexInputRef ref3 = b.field(2, 1, "DNAME");

      RexCall cond1 = (RexCall) b.equals(ref1, ref2);
      RexCall cond2 = (RexCall) b.equals(ref1, ref3);

      RexNode cond = b.and(cond1, cond2);
      return b.semiJoin(cond)
          .project(b.field(0))
          .build();
    };

    relFn(relFn)
        .withRule(
            CoreRules.JOIN_PUSH_EXPRESSIONS,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE,
            CoreRules.JOIN_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Similar to {@link #testJoinConditionPushdown1()} but left join
   * from which more equality conditions can not be inferred. */
  @Test void testJoinConditionPushdown5() {
    final String sql = "select *\n"
        + "from emp e left join dept d\n"
        + "on e.deptno = d.deptno and e.empno = d.deptno";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.PROJECT_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE)
        .checkUnchanged();
  }

  /** Similar to {@link #testJoinConditionPushdown1()} but right join
   * from which more equality conditions can be inferred. */
  @Test void testJoinConditionPushdown6() {
    final String sql = "select *\n"
        + "from emp e right join dept d\n"
        + "on e.deptno = d.deptno and e.empno = d.deptno";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.PROJECT_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE)
        .check();
  }

  /** Tests that filters are combined if they are identical. */
  @Test void testMergeFilter() {
    final String sql = "select name from (\n"
        + "  select *\n"
        + "  from dept\n"
        + "  where deptno = 10)\n"
        + "where deptno = 10\n";
    sql(sql)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_MERGE)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6639">[CALCITE-6639]
   * Optimization that pulls up predicates causes ASOF JOIN validation failures</a>. */
  @Test void testAsofOpt() {
    final String sql = "SELECT *\n"
        + "FROM (VALUES (NULL, 0), (1, NULL), (1, 0), (1, 1), (1, 2), "
        + "(1, 3), (1, 4), (2, 3), (3, 4)) AS t1(k, t)\n"
        + "ASOF JOIN (VALUES (1, NULL), (1, 2), (1, 3), (2, 10), (2, 0)) AS t2(k, t)\n"
        + "MATCH_CONDITION t2.t < t1.t\n"
        + "ON t1.k = t2.k\n";
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  /** Tests to see if the final branch of union is missed. */
  @Test void testUnionMergeRule() {
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
    sql(sql)
        .withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.PROJECT_REMOVE,
            CoreRules.UNION_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6953">[CALCITE-6953]
   * Extend UnionEliminatorRule to support Intersect and Minus</a>. */
  @Test void testUnionEliminatorRuleUnion() {
    final Function<RelBuilder, RelNode> relFn =
        b -> LogicalUnion.create(ImmutableList.of(b.scan("DEPT").build()), true);

    relFn(relFn)
        .withRule(CoreRules.UNION_REMOVE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6953">[CALCITE-6953]
   * Extend UnionEliminatorRule to support Intersect and Minus</a>. */
  @Test void testUnionEliminatorRuleIntersect() {
    final Function<RelBuilder, RelNode> relFn =
        b -> LogicalIntersect.create(ImmutableList.of(b.scan("DEPT").build()), true);

    relFn(relFn)
        .withRule(CoreRules.INTERSECT_REMOVE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6953">[CALCITE-6953]
   * Extend UnionEliminatorRule to support Intersect and Minus</a>. */
  @Test void testUnionEliminatorRuleMinus() {
    final Function<RelBuilder, RelNode> relFn =
        b -> LogicalMinus.create(ImmutableList.of(b.scan("DEPT").build()), true);

    relFn(relFn)
        .withRule(CoreRules.MINUS_REMOVE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6617">[CALCITE-6617]
   * TypeCoercion is not applied correctly to comparisons</a>. */
  @Test void testRand() {
    final String sql = "SELECT * FROM (SELECT 1, ROUND(RAND()) AS A)\n"
        + "WHERE A BETWEEN 1 AND 10 OR A IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)";
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6481">[CALCITE-6481]
   * Optimize 'VALUES...UNION...VALUES' to a single 'VALUES' the IN-list contains CAST
   * and it is converted to VALUES</a>. */
  @Test void testUnionToValuesByInList() {
    final String sql = ""
        + "\n"
        + "with t1(a, y) as (select * from (values (1, 2), (3, null), (7369, null), (7499, 30), (null, 20), (null, 5)) as t1)\n"
        + "select *\n"
        + "from t1\n"
        + "where (t1.a, t1.y) in ((1, 2), (3, null), (7369, null), (7499, 30), (null, 20), (null, 5))";
    sql(sql)
        .withExpand(true)
        .withRelBuilderConfig(b -> b.withSimplifyValues(false))
        .withRule(CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByInList2() {
    final String sql = ""
        + "\n"
        + "with t1(a, y) as (select * from (values (1, 2), (3, null), (7369, null), (7499, 30), (null, 20), (null, 5)) as t1)\n"
        + "select *\n"
        + "from t1\n"
        + "where (t1.a, t1.y) in ((cast(1.1 as int), 2), (3, null), (7369, null), (7499, 30), (null, cast(20.2 as int)), (null, 5))";
    sql(sql)
        .withExpand(true)
        .withRelBuilderConfig(b -> b.withSimplifyValues(false))
        .withRule(CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByInList3() {
    final String sql = "select * from dept where deptno in (12, 34, cast(56.4 as int))";
    sql(sql)
        .withExpand(true)
        .withRelBuilderConfig(b -> b.withSimplifyValues(false))
        .withRule(CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByInList4() {
    final String sql = "select * from dept where deptno in (12, 34, cast(56.4 as double))";
    sql(sql)
        .withExpand(true)
        .withRelBuilderConfig(b -> b.withSimplifyValues(false))
        .withRule(CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByInList5() {
    final String sql = "select deptno in (12, 34, cast(56.4 as double)) from dept";
    sql(sql)
        .withExpand(true)
        .withRelBuilderConfig(b -> b.withSimplifyValues(false))
        .withRule(CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByUnion() {
    final String sql = "select *\n"
        + "from (values (1, 'a'), (2, 'b')) as t(x, y)\n"
        + "union\n"
        + "select *\n"
        + "from (values (1, 'a'), (2, 'b'), (1, 'b'), (2, 'c'), (2, 'c')) as t(x, y)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REMOVE, CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByUnion2() {
    final String sql = "select *\n"
        + "from (values ('a'), ('b')) as t(x)\n"
        + "union\n"
        + "select *\n"
        + "from (values ('a'), ('b'), ('b'), ('c'), ('c')) as t(y)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REMOVE, CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByUnion3() {
    final String sql = "select *\n"
        + "from (values (5.0)) as t(x)\n"
        + "union\n"
        + "select *\n"
        + "from (values (5)) as t(y)";
    sql(sql)
        .withRule(
            CoreRules.PROJECT_REMOVE,
            CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByUnion4() {
    final String sql = "select *\n"
        + "from (values (cast(5.0 as int))) as t(x)\n"
        + "union\n"
        + "select *\n"
        + "from (values (5)) as t(y)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REMOVE, CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByUnionAll() {
    final String sql = "select *\n"
        + "from (values (1, 'a'), (2, 'b')) as t(x, y)\n"
        + "union all\n"
        + "select *\n"
        + "from (values (1, 'a'), (2, 'b'), (1, 'b'), (2, 'c'), (2, 'c')) as t(x, y)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REMOVE, CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByUnionAll2() {
    final String sql = "select *\n"
        + "from (values ('a'), ('b')) as t(x)\n"
        + "union all\n"
        + "select *\n"
        + "from (values ('a'), ('b'), ('b'), ('c'), ('c')) as t(y)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REMOVE, CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByUnionAll3() {
    final String sql = "select *\n"
        + "from (values (5.0)) as t(x)\n"
        + "union all\n"
        + "select *\n"
        + "from (values (5)) as t(y)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REMOVE,
            CoreRules.PROJECT_VALUES_MERGE,
            CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testUnionToValuesByUnionAll4() {
    final String sql = "select *\n"
        + "from (values (cast(5.0 as int))) as t(x)\n"
        + "union all\n"
        + "select *\n"
        + "from (values (5)) as t(y)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REMOVE, CoreRules.UNION_TO_VALUES)
        .withInSubQueryThreshold(0)
        .check();
  }

  @Test void testMinusMergeRule() {
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
    sql(sql)
        .withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.PROJECT_REMOVE,
            CoreRules.MINUS_MERGE)
        .check();
  }

  /** Tests that a filters is combined are combined if they are identical,
   * even if one of them originates in an ON clause of a JOIN. */
  @Test void testMergeJoinFilter() {
    final String sql = "select * from (\n"
        + "  select d.deptno, e.ename\n"
        + "  from emp as e\n"
        + "  join dept as d\n"
        + "  on e.deptno = d.deptno\n"
        + "  and d.deptno = 10)\n"
        + "where deptno = 10\n";
    sql(sql)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_MERGE,
            CoreRules.FILTER_INTO_JOIN)
        .check();
  }

  /** Tests {@link UnionMergeRule}, which merges 2 {@link Union} operators into
   * a single {@code Union} with 3 inputs. */
  @Test void testMergeUnionAll() {
    final String sql = "select * from emp where deptno = 10\n"
        + "union all\n"
        + "select * from emp where deptno = 20\n"
        + "union all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.UNION_MERGE)
        .check();
  }

  /** Tests {@link UnionMergeRule}, which merges 2 {@link Union}
   * {@code DISTINCT} (not {@code ALL}) operators into a single
   * {@code Union} with 3 inputs. */
  @Test void testMergeUnionDistinct() {
    final String sql = "select * from emp where deptno = 10\n"
        + "union distinct\n"
        + "select * from emp where deptno = 20\n"
        + "union\n" // same as 'union distinct'
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.UNION_MERGE)
        .check();
  }

  /** Tests that {@link UnionMergeRule} does nothing if its arguments have
   * different {@code ALL} settings. */
  @Test void testMergeUnionMixed() {
    final String sql = "select * from emp where deptno = 10\n"
        + "union\n"
        + "select * from emp where deptno = 20\n"
        + "union all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.UNION_MERGE)
        .checkUnchanged();
  }

  /** Tests that {@link UnionMergeRule} converts all inputs to DISTINCT
   * if the top one is DISTINCT.
   * (Since UNION is left-associative, the "top one" is the rightmost.) */
  @Test void testMergeUnionMixed2() {
    final String sql = "select * from emp where deptno = 10\n"
        + "union all\n"
        + "select * from emp where deptno = 20\n"
        + "union\n"
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.UNION_MERGE)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-2067">
   * [CALCITE-2067] RexLiteral cannot represent accurately floating point values,
   * including NaN, Infinity</a>. */
  @Test public void testDoubleReduction() {
    // Without the fix for CALCITE-2067 the result returned below is
    // 1008618.49.  Ironically, that result is more accurate; however
    // it is not the result returned by the pow() function, which is
    // 1008618.4899999999
    final String sql = "SELECT power(1004.3, 2)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-2067">
   * [CALCITE-2067] RexLiteral cannot represent accurately floating point values,
   * including NaN, Infinity</a>. */
  @Test public void testDoubleReduction2() {
    // Without the fix for CALCITE-2067 the following expression is not
    // reduced to Infinity, since Infinity cannot be represented
    // as a BigDecimal value.
    final String sql2 = "SELECT 1.0 / 0.0e0";
    sql(sql2)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Tests that {@link UnionMergeRule} does nothing if its arguments have
   * are different set operators, {@link Union} and {@link Intersect}. */
  @Test void testMergeSetOpMixed() {
    final String sql = "select * from emp where deptno = 10\n"
        + "union\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.UNION_MERGE,
            CoreRules.INTERSECT_MERGE)
        .checkUnchanged();
  }

  /** Tests {@link CoreRules#INTERSECT_MERGE}, which merges 2
   * {@link Intersect} operators into a single {@code Intersect} with 3
   * inputs. */
  @Test void testMergeIntersect() {
    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.INTERSECT_MERGE)
        .check();
  }

  /** Tests {@link org.apache.calcite.rel.rules.IntersectToDistinctRule},
   * which rewrites an {@link Intersect} operator with 3 inputs. */
  @Test void testIntersectToDistinct() {
    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect\n"
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.INTERSECT_MERGE,
            CoreRules.INTERSECT_TO_DISTINCT)
        .check();
  }

  /** Tests that {@link org.apache.calcite.rel.rules.IntersectToDistinctRule}
   * correctly ignores an {@code INTERSECT ALL}. It can only handle
   * {@code INTERSECT DISTINCT}. */
  @Test void testIntersectToDistinctAll() {
    final String sql = "select * from emp where deptno = 10\n"
        + "intersect\n"
        + "select * from emp where deptno = 20\n"
        + "intersect all\n"
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.INTERSECT_MERGE,
            CoreRules.INTERSECT_TO_DISTINCT)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6880">
   * [CALCITE-6880] Implement IntersectToSemiJoinRule</a>. */
  @Test void testIntersectToSemiJoin() {
    final String sql = "select ename from emp where deptno = 10\n"
        + "intersect\n"
        + "select ename from emp where deptno = 20\n";
    sql(sql).withRule(CoreRules.INTERSECT_TO_SEMI_JOIN)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7000">[CALCITE-7000]
   * Extend IntersectToSemiJoinRule to support n-way inputs</a>. */
  @Test void testIntersectToSemiJoin2() {
    final String sql = "select ename from emp where deptno = 10\n"
        + "intersect\n"
        + "select deptno from emp where ename in ('a', 'b')\n"
        + "intersect\n"
        + "select ename from empnullables\n";
    sql(sql).withPreRule(CoreRules.INTERSECT_MERGE)
        .withRule(CoreRules.INTERSECT_TO_SEMI_JOIN)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6880">
   * [CALCITE-6880] Implement IntersectToSemiJoinRule</a>. */
  @Test void testIntersectToSemiJoinMultiCol() {
    final String sql = "select deptno, ename from emp where deptno = 10\n"
        + "intersect\n"
        + "select deptno, ename from emp where deptno = 20\n";
    sql(sql).withRule(CoreRules.INTERSECT_TO_SEMI_JOIN)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6880">
   * [CALCITE-6880] Implement IntersectToSemiJoinRule</a>. */
  @Test void testIntersectToSemiJoinAll() {
    final String sql = "select ename from emp where deptno = 10\n"
        + "intersect\n"
        + "select ename from emp where deptno = 20\n"
        + "intersect all\n"
        + "select ename from emp where deptno = 30\n";
    sql(sql).withRule(CoreRules.INTERSECT_TO_SEMI_JOIN)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6880">
   * [CALCITE-6880] Implement IntersectToSemiJoinRule</a>. */
  @Test void testIntersectToSemiJoinCoercion() {
    final String sql = "select ename from emp where deptno = 10\n"
        + "intersect\n"
        + "select job from emp where deptno = 20\n";
    sql(sql).withRule(CoreRules.INTERSECT_TO_SEMI_JOIN)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6948">[CALCITE-6948]
   * Implement MinusToAntiJoinRule</a>. */
  @Test void testMinusToAntiJoinRule() {
    final String sql = "select ename from emp where deptno = 10\n"
        + "except\n"
        + "select ename from emp where deptno = 20\n";
    sql(sql).withRule(CoreRules.MINUS_TO_ANTI_JOIN)
        .check();
  }

  @Test void testMinusToAntiJoinRuleMultiInputs() {
    final String sql = "select ename from emp where deptno = 10\n"
        + "except\n"
        + "select deptno from emp where ename in ('a', 'b')\n"
        + "except\n"
        + "select ename from empnullables\n";
    sql(sql).withPreRule(CoreRules.MINUS_MERGE)
        .withRule(CoreRules.MINUS_TO_ANTI_JOIN)
        .check();
  }

  /** Tests {@link CoreRules#MINUS_MERGE}, which merges 2
   * {@link Minus} operators into a single {@code Minus} with 3
   * inputs. */
  @Test void testMergeMinus() {
    final String sql = "select * from emp where deptno = 10\n"
        + "except\n"
        + "select * from emp where deptno = 20\n"
        + "except\n"
        + "select * from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.MINUS_MERGE)
        .check();
  }

  /** Tests {@link org.apache.calcite.rel.rules.MinusToDistinctRule},
   * which rewrites an {@link Minus} operator with 3 inputs. */
  @Test void testMinusToDistinct() {
    final String sql = "select EMPNO, ENAME, JOB from emp where deptno = 10\n"
        + "except\n"
        + "select EMPNO, ENAME, JOB from emp where deptno = 20\n"
        + "except\n"
        + "select EMPNO, ENAME, JOB from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.MINUS_MERGE, CoreRules.MINUS_TO_DISTINCT, CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Tests {@link org.apache.calcite.rel.rules.MinusToDistinctRule},
   *  correctly ignores an {@code EXCEPT ALL}. It can only handle
   *  {@code EXCEPT DISTINCT}. */
  @Test void testMinusToDistinctAll() {
    final String sql = "select EMPNO, ENAME, JOB from emp where deptno = 10\n"
        + "except\n"
        + "select EMPNO, ENAME, JOB from emp where deptno = 20\n"
        + "except all\n"
        + "select EMPNO, ENAME, JOB from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.MINUS_MERGE, CoreRules.MINUS_TO_DISTINCT, CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Tests {@link org.apache.calcite.rel.rules.MinusToDistinctRule},
   *  correctly ignores an {@code EXCEPT ALL}. It can only handle
   *  {@code EXCEPT DISTINCT}. */
  @Test void testMinusToDistinctAllAndDistinct() {
    final String sql = "select EMPNO, ENAME, JOB from emp where deptno = 10\n"
        + "except all\n"
        + "select EMPNO, ENAME, JOB from emp where deptno = 20\n"
        + "except\n"
        + "select EMPNO, ENAME, JOB from emp where deptno = 30\n";
    sql(sql)
        .withRule(CoreRules.MINUS_TO_DISTINCT, CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Tests {@link CoreRules#MINUS_MERGE}
   * does not merge {@code Minus(a, Minus(b, c))}
   * into {@code Minus(a, b, c)}, which would be incorrect. */
  @Test void testMergeMinusRightDeep() {
    final String sql = "select * from emp where deptno = 10\n"
        + "except\n"
        + "select * from (\n"
        + "  select * from emp where deptno = 20\n"
        + "  except\n"
        + "  select * from emp where deptno = 30)";
    sql(sql)
        .withRule(CoreRules.MINUS_MERGE)
        .checkUnchanged();
  }

  @Test void testHeterogeneousConversion() {
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
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();

    final String sql = "select upper(ename) from emp union all\n"
        + "select lower(ename) from emp";
    sql(sql).withProgram(program).check();
  }

  @Test void testPushSemiJoinPastJoinRuleLeft() {
    // tests the case where the semijoin is pushed to the left
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and e1.empno = e2.empno";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
            CoreRules.SEMI_JOIN_JOIN_TRANSPOSE)
        .check();
  }

  @Test void testPushSemiJoinPastJoinRuleRight() {
    // tests the case where the semijoin is pushed to the right
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
            CoreRules.SEMI_JOIN_JOIN_TRANSPOSE)
        .check();
  }

  @Test void testPushSemiJoinPastJoinRuleNotHappensJoinKeysDifferentOrigin() {
    // tests the case where the semijoin is not pushed because it uses join keys from both tables
    // of the bottom join.
    final String sql = "select e.ename from emp e, dept d, bonus b\n"
        + "where e.deptno = d.deptno and e.ename = b.ename and d.name = b.job";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
            CoreRules.SEMI_JOIN_JOIN_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5952">[CALCITE-5952]
   * SemiJoinJoinTransposeRule should check if JoinType supports pushing predicates
   * into its inputs</a>. */
  @Test void testPushSemiJoinToLeftJoinLeftInput() {
    // tests the case that semijoin can be pushed to the left input of the left join
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("DEPT")
        .scan("EMP")
        .join(JoinRelType.LEFT,
            b.equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO"))
        )
        .scan("BONUS")
        .semiJoin(
            b.equals(
                b.field(2, 0, "DNAME"),
                b.field(2, 1, "JOB")))
        .build();
    relFn(relFn).withRule(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5952">[CALCITE-5952]
   * SemiJoinJoinTransposeRule should check if JoinType supports pushing predicates
   * into its inputs</a>. */
  @Test void testPushSemiJoinToRightJoinRightInput() {
    // tests the case that semijoin can be pushed to the right input of the right join
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.RIGHT,
            b.equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO"))
        )
        .scan("BONUS")
        .semiJoin(
            b.equals(
                b.field(2, 0, "DNAME"),
                b.field(2, 1, "JOB")))
        .build();
    relFn(relFn).withRule(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5952">[CALCITE-5952]
   * SemiJoinJoinTransposeRule should check if JoinType supports pushing predicates
   * into its inputs</a>. */
  @Test void testCanNotPushSemiJoinToLeftJoinRightInput() {
    // tests the case that semijoin cannot be pushed to the right input of the left join
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.LEFT,
            b.equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO"))
        )
        .scan("BONUS")
        .semiJoin(
            b.equals(
                b.field(2, 0, "DNAME"),
                b.field(2, 1, "JOB")))
        .build();
    relFn(relFn).withRule(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5952">[CALCITE-5952]
   * SemiJoinJoinTransposeRule should check if JoinType supports pushing predicates
   * into its inputs</a>. */
  @Test void testCanNotPushSemiJoinToRightJoinLeftInput() {
    // tests the case that semijoin cannot be pushed to the left input of the right join
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("DEPT")
        .scan("EMP")
        .join(JoinRelType.RIGHT,
            b.equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO"))
        )
        .scan("BONUS")
        .semiJoin(
            b.equals(
                b.field(2, 0, "DNAME"),
                b.field(2, 1, "JOB")))
        .build();
    relFn(relFn).withRule(CoreRules.SEMI_JOIN_JOIN_TRANSPOSE).checkUnchanged();
  }

  @Test void testPushSemiJoinPastFilter() {
    final String sql = "select e.ename from emp e, dept d\n"
        + "where e.deptno = d.deptno and e.ename = 'foo'";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
            CoreRules.SEMI_JOIN_FILTER_TRANSPOSE)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5289">
   * [CALCITE-5289] Assertion failure in MultiJoinOptimizeBushyRule</a>. */
  @Test void testBushyJoinRule() {
    final String sql = "select emp.ename from emp LEFT JOIN emp AS emp1 on emp.ename = emp1.ename";
    sql(sql).withPreRule(CoreRules.JOIN_TO_MULTI_JOIN)
        .withRule(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY)
        .checkUnchanged();
  }

  @Test void testConvertMultiJoinRule() {
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    sql(sql).withProgram(program).check();
  }

  @Test void testManyFiltersOnTopOfMultiJoinShouldCollapse() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .addRuleCollection(
            Arrays.asList(CoreRules.FILTER_MULTI_JOIN_MERGE,
                CoreRules.PROJECT_MULTI_JOIN_MERGE))
        .build();
    final String sql = "select * from (select * from emp e1 left outer join dept d\n"
        + "on e1.deptno = d.deptno\n"
        + "where d.deptno > 3) where ename LIKE 'bar'";
    sql(sql).withProgram(program).check();
  }

  @Test void testReduceConstants() {
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
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .withRelBuilderSimplify(false)
        .check();
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7070">[CALCITE-7070]
   * FILTER_REDUCE_EXPRESSIONS crashes on expression BETWEEN ( NULL) AND X</a>. */
  @Test void testFilterReduceCrash() {
    final String sql = "SELECT + 54 FROM emp WHERE NOT CAST ( CAST(empno AS DOUBLE) AS INTEGER ) "
        + "NOT BETWEEN ( NULL ) AND 89";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-570">[CALCITE-570]
   * ReduceExpressionsRule throws "duplicate key" exception</a>. */
  @Test void testReduceConstantsDup() {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where d.deptno=7 and d.deptno=8";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-935">[CALCITE-935]
   * Improve how ReduceExpressionsRule handles duplicate constraints</a>. */
  @Test void testReduceConstantsDup2() {
    final String sql = "select *\n"
        + "from emp\n"
        + "where deptno=7 and deptno=8\n"
        + "and empno = 10 and mgr is null and empno = 10";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDup3() {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where d.deptno<>7 or d.deptno<>8";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDup3Null() {
    final String sql = "select e.empno"
        + " from emp e"
        + " where e.mgr<>7 or e.mgr<>8";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot() {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where not(d.deptno=7 and d.deptno=8)";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNotNull() {
    final String sql = "select e.empno"
        + " from emp e"
        + " where not(e.mgr=7 and e.mgr=8)";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot2() {
    final String sql = "select d.deptno"
        + " from dept d"
        + " where not(d.deptno=7 and d.name='foo' and d.deptno=8)";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3198">[CALCITE-3198]
   * Enhance RexSimplify to handle (x&lt;&gt;a or x&lt;&gt;b)</a>. */
  @Test void testReduceConstantsDupNot2Null() {
    final String sql = "select e.empno"
        + " from emp e"
        + " where not(e.mgr=7 and e.deptno=8 and e.mgr=8)";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testPullNull() {
    final String sql = "select *\n"
        + "from emp\n"
        + "where deptno=7\n"
        + "and empno = 10 and mgr is null and empno = 10";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testOrAlwaysTrue() {
    final String sql = "select * from EMPNULLABLES_20\n"
        + "where sal is null or sal is not null";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testOrAlwaysTrue2() {
    final String sql = "select * from EMPNULLABLES_20\n"
        + "where sal is not null or sal is null";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceConstants2() {
    final String sql = "select p1 is not distinct from p0\n"
        + "from (values (2, cast(null as integer))) as t(p0, p1)";
    sql(sql)
        .withRelBuilderConfig(b -> b.withSimplifyValues(false))
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  @Test void testReduceConstants3() {
    final String sql = "select e.mgr is not distinct from f.mgr "
        + "from emp e join emp f on (e.mgr=f.mgr) where e.mgr is null";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-902">[CALCITE-902]
   * Match nullability when reducing expressions in a Project</a>. */
  @Test void testReduceConstantsProjectNullable() {
    final String sql = "select mgr from emp where mgr=10";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  // see HIVE-9645
  @Test void testReduceConstantsNullEqualsOne() {
    final String sql = "select count(1) from emp where cast(null as integer) = 1";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  // see HIVE-9644
  @Test void testReduceConstantsCaseEquals() {
    final String sql = "select count(1) from emp\n"
        + "where case deptno\n"
        + "  when 20 then 2\n"
        + "  when 10 then 1\n"
        + "  else 3 end = 1";
    // Equivalent to 'deptno = 10'
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceConstantsCaseEquals2() {
    final String sql = "select count(1) from emp\n"
        + "where case deptno\n"
        + "  when 20 then 2\n"
        + "  when 10 then 1\n"
        + "  else cast(null as integer) end = 1";

    // Equivalent to 'case when deptno = 20 then false
    //                     when deptno = 10 then true
    //                     else null end'
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceConstantsCaseEquals3() {
    final String sql = "select count(1) from emp\n"
        + "where case deptno\n"
        + "  when 30 then 1\n"
        + "  when 20 then 2\n"
        + "  when 10 then 1\n"
        + "  when 30 then 111\n"
        + "  else 0 end = 1";

    // Equivalent to 'deptno = 30 or deptno = 10'
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testSkipReduceConstantsCaseEquals() {
    final String sql = "select * from emp e1, emp e2\n"
        + "where coalesce(e1.mgr, -1) = coalesce(e2.mgr, -1)";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_INTO_JOIN)
        .check();
  }

  @Test void testReduceConstantsEliminatesFilter() {
    final String sql = "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)";

    // WHERE NULL is the same as WHERE FALSE, so get empty result
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1860">[CALCITE-1860]
   * Duplicate null predicates cause NullPointerException in RexUtil</a>. */
  @Test void testReduceConstantsNull() {
    final String sql = "select * from (\n"
        + "  select *\n"
        + "  from (\n"
        + "    select cast(null as integer) as n\n"
        + "    from emp)\n"
        + "  where n is null and n is null)\n"
        + "where n is null";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-566">[CALCITE-566]
   * ReduceExpressionsRule requires planner to have an Executor</a>. */
  @Test void testReduceConstantsRequiresExecutor() {
    // Rule should not fire, but there should be no NPE
    // Create a new planner instance, so we can remove its executor without
    // breaking other tests.
    final RelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    final String sql =
        "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)";
    sql(sql)
        .withFactory(t -> t.withPlannerFactory(context -> planner))
        .withBefore((fixture, r) -> {
          // Remove the executor
          assertThat(r.getCluster().getPlanner(), sameInstance(planner));
          r.getCluster().getPlanner().setExecutor(null);
          return r;
        })
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testAlreadyFalseEliminatesFilter() {
    final String sql = "select * from (values (1,2)) where false";

    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceConstantsCalc() {
    // This reduction does not work using
    // ReduceExpressionsRule.PROJECT_INSTANCE or FILTER_INSTANCE,
    // only CALC_INSTANCE, because we need to pull the project expression
    //    upper('table')
    // into the condition
    //    upper('table') = 'TABLE'
    // and reduce it to TRUE. Only in the Calc are projects and conditions
    // combined.
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE)
        .addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS)

            // the hard part is done... a few more rule calls to clean up
        .addRuleInstance(PruneEmptyRules.UNION_INSTANCE)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE)
        .addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS)
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
    sql(sql)
        .withRelBuilderConfig(c -> c.withSimplifyValues(false))
        .withProgram(program).check();
  }

  @Test void testReducingConstantsInferredFromCorrelate() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .build();

    final String sql = "SELECT ename,\n"
        + "  empno,\n"
        + "  T.empno_r,\n"
        + "  CASE\n"
        + "    WHEN __source__type__ = 'bounded'\n"
        + "      THEN 1\n"
        + "    ELSE 2\n"
        + "    END AS type\n"
        + "FROM (\n"
        + "  SELECT ename,\n"
        + "    empno,\n"
        + "    'bounded' AS __source__type__\n"
        + "  FROM emp\n"
        + "  ) a,\n"
        + "  lateral TABLE (ramp(empno)) AS T(empno_r)";
    sql(sql)
        .withRelBuilderConfig(c -> c.withSimplifyValues(false))
        .withProgram(program).check();
  }


  @Test void testRemoveSemiJoin() {
    final String sql = "select e.ename from emp e, dept d\n"
        + "where e.deptno = d.deptno";
    sql(sql)
        .withPreRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN)
        .withRule(CoreRules.SEMI_JOIN_REMOVE)
        .check();
  }

  @Test void testRemoveSemiJoinWithFilter() {
    final String sql = "select e.ename from emp e, dept d\n"
        + "where e.deptno = d.deptno and e.ename = 'foo'";
    sql(sql)
        .withPreRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
            CoreRules.SEMI_JOIN_FILTER_TRANSPOSE)
        .withRule(CoreRules.SEMI_JOIN_REMOVE)
        .check();
  }

  @Test void testRemoveSemiJoinRight() {
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno";
    sql(sql)
        .withPreRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
            CoreRules.SEMI_JOIN_JOIN_TRANSPOSE)
        .withRule(CoreRules.SEMI_JOIN_REMOVE)
        .check();
  }

  @Test void testRemoveSemiJoinRightWithFilter() {
    final String sql = "select e1.ename from emp e1, dept d, emp e2\n"
        + "where e1.deptno = d.deptno and d.deptno = e2.deptno\n"
        + "and d.name = 'foo'";
    sql(sql)
        .withPreRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
            CoreRules.SEMI_JOIN_JOIN_TRANSPOSE,
            CoreRules.SEMI_JOIN_FILTER_TRANSPOSE)
        .withRule(CoreRules.SEMI_JOIN_REMOVE)
        .check();
  }

  /** Creates an environment for testing multi-join queries. */
  private RelOptFixture multiJoin(String query) {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.PROJECT_REMOVE)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();
    return sql(query)
        .withCatalogReaderFactory((typeFactory, caseSensitive) ->
            new MockCatalogReader(typeFactory, caseSensitive) {
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
            }.init())
        .withProgram(program);
  }

  @Test void testConvertMultiJoinRuleOuterJoins() {
    final String sql = "select * from "
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
        + "    on a = i and h = j";
    multiJoin(sql).check();
  }

  @Test void testConvertMultiJoinRuleOuterJoins2() {
    // in (A right join B) join C, pushing C is not allowed;
    // therefore there should be 2 MultiJoin
    multiJoin("select * from A right join B on a = b join C on b = c")
        .check();
  }

  @Test void testConvertMultiJoinRuleOuterJoins3() {
    // in (A join B) left join C, pushing C is allowed;
    // therefore there should be 1 MultiJoin
    multiJoin("select * from A join B on a = b left join C on b = c")
        .check();
  }

  @Test void testConvertMultiJoinRuleOuterJoins4() {
    // in (A join B) right join C, pushing C is not allowed;
    // therefore there should be 2 MultiJoin
    multiJoin("select * from A join B on a = b right join C on b = c")
        .check();
  }

  @Test void testPushSemiJoinPastProject() {
    final String sql = "select e.* from\n"
        + "(select ename, trim(job), sal * 2, deptno from emp) e, dept d\n"
        + "where e.deptno = d.deptno";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN,
            CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE)
        .check();
  }

  @Test void testReduceValuesUnderFilter() {
    // Plan should be same as for
    // select a, b from (values (10,'x')) as t(a, b)");
    final String sql = "select a, b from (values (10, 'x'), (20, 'y')) as t(a, b) where a < 15";
    sql(sql)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_VALUES_MERGE)
        .check();
  }

  @Test void testReduceValuesUnderProject() {
    // Plan should be same as for
    // select a, b as x from (values (11), (23)) as t(x)");
    final String sql = "select a + b from (values (10, 1), (20, 3)) as t(a, b)";
    sql(sql)
        .withRule(CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_VALUES_MERGE)
        .check();
  }

  @Test void testReduceValuesUnderProjectFilter() {
    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    final String sql = "select a + b as x, b, a\n"
        + "from (values (10, 1), (30, 7), (20, 3)) as t(a, b)\n"
        + "where a - b < 21";
    sql(sql).withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.PROJECT_MERGE,
        CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1439">[CALCITE-1439]
   * Handling errors during constant reduction</a>. */
  @Test void testReduceCase() {
    final String sql = "select\n"
        + "  case when false then cast(2.1 as float)\n"
        + "   else cast(1 as integer) end as newcol\n"
        + "from emp";
    sql(sql).withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .withRelBuilderSimplify(false)
        .check();
  }

  private void checkReduceNullableToNotNull(ReduceExpressionsRule<?> rule) {
    final String sql = "select\n"
        + "  empno + case when 'a' = 'a' then 1 else null end as newcol\n"
        + "from emp";
    sql(sql).withRule(rule)
        .withRelBuilderSimplify(false)
        .check();
  }

  /** Test case that reduces a nullable expression to a NOT NULL literal that
   * is cast to nullable. */
  @Test void testReduceNullableToNotNull() {
    checkReduceNullableToNotNull(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
  }

  /** Test case that reduces a nullable expression to a NOT NULL literal. */
  @Test void testReduceNullableToNotNull2() {
    final ProjectReduceExpressionsRule rule =
        CoreRules.PROJECT_REDUCE_EXPRESSIONS.config
            .withOperandFor(LogicalProject.class)
            .withMatchNullability(false)
            .as(ProjectReduceExpressionsRule.ProjectReduceExpressionsRuleConfig.class)
            .toRule();
    checkReduceNullableToNotNull(rule);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2736">[CALCITE-2736]
   * ReduceExpressionsRule never reduces dynamic expressions but this should be
   * configurable</a>. Tests that a dynamic function (USER) is reduced if and
   * only if {@link ReduceExpressionsRule.Config#treatDynamicCallsAsConstant()}
   * is true. */
  @Test void testReduceDynamic() {
    checkDynamicFunctions(true).check();
  }

  /** As {@link #testReduceDynamic()}. */
  @Test void testNoReduceDynamic() {
    checkDynamicFunctions(false).checkUnchanged();
  }

  RelOptFixture checkDynamicFunctions(boolean treatDynamicCallsAsConstant) {
    // Create a customized executor with given context operator that reduces
    // "USER" to "happyCalciteUser"
    final RexExecutorImpl executor =
        new RexExecutorImpl(
            DataContexts.of(name ->
                name.equals(DataContext.Variable.USER.camelName)
                    ? "happyCalciteUser"
                    : fail("unknown: " + name)));

    RelOptPlanner planner = new MockRelOptPlanner(Contexts.empty());
    planner.setExecutor(executor);

    final ReduceExpressionsRule<?> rule =
        CoreRules.PROJECT_REDUCE_EXPRESSIONS.config
            .withOperandFor(LogicalProject.class)
            .withTreatDynamicCallsAsConstant(treatDynamicCallsAsConstant)
            .as(ProjectReduceExpressionsRule.Config.class)
            .toRule();

    final String sql = "select USER from emp";
    return sql(sql)
        .withFactory(t -> t.withPlannerFactory(context -> planner))
        .withRule(rule);
  }

  @Test void testReduceConstantsIsNull() {
    final String sql = "select empno from emp where empno=10 and empno is null";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceConstantsIsNotNull() {
    final String sql = "select empno from emp\n"
        + "where empno=10 and empno is not null";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceConstantsNegated() {
    final String sql = "select empno from emp\n"
        + "where empno=10 and not(empno=10)";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceConstantsNegatedInverted() {
    final String sql = "select empno from emp where empno>10 and empno<=10";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2638">[CALCITE-2638]
   * Constant reducer must not duplicate calls to non-deterministic
   * functions</a>. */
  @Test void testReduceConstantsNonDeterministicFunction() {
    final SqlOperator nonDeterministicOp =
        SqlBasicFunction.create("NDC", ReturnTypes.INTEGER,
            OperandTypes.VARIADIC).withDeterministic(false);

    // Build a tree equivalent to the SQL
    //  SELECT sal, n
    //  FROM (SELECT sal, NDC() AS n FROM emp)
    //  WHERE n > 10
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .project(b.field("SAL"),
                b.alias(b.call(nonDeterministicOp), "N"))
            .filter(b.greaterThan(b.field("N"), b.literal(10)))
            .build();

    relFn(relFn)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS, CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  /** Checks that constant reducer duplicates calls to dynamic functions, if
   * appropriate. CURRENT_TIMESTAMP is a dynamic function. */
  @Test void testReduceConstantsDynamicFunction() {
    final String sql = "select sal, t\n"
        + "from (select sal, current_timestamp t from emp)\n"
        + "where t > TIMESTAMP '2018-01-01 00:00:00'";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  @Test void testCasePushIsAlwaysWorking() {
    final String sql = "select empno from emp"
        + " where case when sal > 1000 then empno else sal end = 1";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.CALC_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceValuesNull() {
    // The NULL literal presents pitfalls for value-reduction. Only
    // an INSERT statement contains un-CASTed NULL values.
    final String sql = "insert into EMPNULLABLES(EMPNO, ENAME, JOB) (select 0, 'null', NULL)";
    sql(sql).withRule(CoreRules.PROJECT_VALUES_MERGE).check();
  }

  @Test void testReduceValuesToEmpty() {
    // Plan should be same as for
    // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
    final String sql = "select a + b as x, b, a from (values (10, 1), (30, 7)) as t(a, b)\n"
        + "where a - b < 0";
    sql(sql)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .check();
  }

  @Test void testReduceConstantsWindow() {
    final String sql = "select col1, col2, col3\n"
        + "from (\n"
        + "  select empno,\n"
        + "    sum(100) over (partition by deptno, sal order by sal) as col1,\n"
        + "    sum(100) over (partition by sal order by deptno) as col2,\n"
        + "    sum(sal) over (partition by deptno order by sal) as col3\n"
        + "  from emp where sal = 5000)";

    sql(sql)
        .withRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_WINDOW_TRANSPOSE,
            CoreRules.WINDOW_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceConstantsWithMultipleOrderByWindow() {
    final String sql = "select col1, col2\n"
        + "from (\n"
        + "  select empno,\n"
        + "    sum(100) over (order by deptno, empno range between current row and unbounded following) as col1,\n"
        + "    sum(100) over (partition by sal, deptno order by deptno, empno range between unbounded preceding and unbounded following) as col2\n"
        + "  from emp where sal = 5000)";

    sql(sql)
        .withRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_WINDOW_TRANSPOSE,
            CoreRules.WINDOW_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testProjectOverWithAvg() {
    final String sql = "select avg(sal) over (order by empno rows 3 preceding) from emp";

    sql(sql)
        .withRule(CoreRules.PROJECT_OVER_SUM_TO_SUM0_RULE,
            CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_WINDOW_TRANSPOSE,
            CoreRules.WINDOW_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testEmptyFilterProjectUnion() {
    // Plan should be same as for
    // select * from (values (30, 3)) as t(x, y)");
    final String sql = "select * from (\n"
        + "select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "union all\n"
        + "select * from (values (20, 2))\n"
        + ")\n"
        + "where x + y > 30";
    sql(sql)
        .withRule(CoreRules.FILTER_SET_OP_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_FILTER_VALUES_MERGE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.UNION_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6955">[CALCITE-6955]
   * PruneEmptyRules does not handle the all attribute of SetOp correctly</a>. */
  @Test void testEmptyFilterProjectUnion2() {
    final String sql = "select * from (\n"
        + "select * from (values (10, 1), (30, 3), (30, 3)) as t (x, y)\n"
        + "union\n"
        + "select * from (values (20, 2))\n"
        + ")\n"
        + "where x + y > 30";
    sql(sql)
        .withRule(CoreRules.FILTER_SET_OP_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_FILTER_VALUES_MERGE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.UNION_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1488">[CALCITE-1488]
   * ValuesReduceRule should ignore empty Values</a>. */
  @Test void testEmptyProject() {
    final String sql = "select z + x from (\n"
        + "  select x + y as z, x from (\n"
        + "    select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "    where x + y > 50))";
    sql(sql)
        .withRule(CoreRules.PROJECT_FILTER_VALUES_MERGE,
            CoreRules.FILTER_VALUES_MERGE,
            CoreRules.PROJECT_VALUES_MERGE)
        .check();
  }

  /** Same query as {@link #testEmptyProject()}, and {@link PruneEmptyRules}
   * is able to do the job that {@link ValuesReduceRule} cannot do. */
  @Test void testEmptyProject2() {
    final String sql = "select z + x from (\n"
        + "  select x + y as z, x from (\n"
        + "    select * from (values (10, 1), (30, 3)) as t (x, y)\n"
        + "    where x + y > 50))";
    sql(sql)
        .withRule(CoreRules.FILTER_VALUES_MERGE,
            PruneEmptyRules.PROJECT_INSTANCE)
        .check();
  }

  @Test void testEmptyIntersect() {
    final String sql = "select * from (values (30, 3))"
        + "intersect\n"
        + "select *\nfrom (values (10, 1), (30, 3)) as t (x, y) where x > 50\n"
        + "intersect\n"
        + "select * from (values (30, 3))";
    sql(sql)
        .withRule(CoreRules.PROJECT_FILTER_VALUES_MERGE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.INTERSECT_INSTANCE)
        .check();
  }

  @Test void testEmptyMinus() {
    // First input is empty; therefore whole expression is empty
    final String sql = "select * from (values (30, 3)) as t (x, y)\n"
        + "where x > 30\n"
        + "except\n"
        + "select * from (values (20, 2))\n"
        + "except\n"
        + "select * from (values (40, 4))";
    sql(sql)
        .withRule(CoreRules.PROJECT_FILTER_VALUES_MERGE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.MINUS_INSTANCE)
        .check();
  }

  @Test void testEmptyMinus2() {
    // Second and fourth inputs are empty; they are removed
    final String sql = "select * from (values (30, 3)) as t (x, y)\n"
        + "except\n"
        + "select * from (values (20, 2)) as t (x, y) where x > 30\n"
        + "except\n"
        + "select * from (values (40, 4))\n"
        + "except\n"
        + "select * from (values (50, 5)) as t (x, y) where x > 50";
    sql(sql)
        .withRule(CoreRules.PROJECT_FILTER_VALUES_MERGE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.MINUS_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6955">[CALCITE-6955]
   * PruneEmptyRules does not handle the all attribute of SetOp correctly</a>. */
  @Test void testEmptyMinus3() {
    // First input is empty; therefore whole expression is empty
    final String sql = "select * from (values (30, 3), (30, 3)) as t (x, y)\n"
        + "except\n"
        + "select * from (values (20, 2)) as t (x, y) where x > 30";
    sql(sql)
        .withRule(CoreRules.PROJECT_FILTER_VALUES_MERGE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.MINUS_INSTANCE)
        .check();
  }

  @Test void testEmptyTable() {
    // table is transformed to empty values and extra project will be removed.
    final String sql = "select * from EMPTY_PRODUCTS\n";
    sql(sql)
        .withRule(
            PruneEmptyRules.EMPTY_TABLE_INSTANCE,
            PruneEmptyRules.PROJECT_INSTANCE)
        .check();
  }

  @Test void testJoinWithConstantRowOnRight() {
    final String sql = "select e.empno, e.ename, c.*"
        + " from emp e cross join (select 5, 5) c";

    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_RIGHT_INSTANCE)
        .check();
  }

  @Test void testJoinWithConstantRowOnLeft() {
    final String sql = "select e.empno, e.ename, c.*"
        + " from (select 5, 5) c cross join emp e";

    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_LEFT_INSTANCE)
        .check();
  }

  @Test void testInnerJoinWithConstantRowOnLeft() {
    final String sql = "select e.empno, e.ename, c.*"
        + " from (select 5 as nm) c inner join emp e on e.empno = c.nm";

    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_LEFT_INSTANCE)
        .check();
  }

  @Test void testInnerJoinWithConstantRowOnRight() {
    final String sql = "select e.empno, e.ename, c.*"
        + " from emp e inner join (select 5 as nm) c  on e.empno = c.nm";

    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_RIGHT_INSTANCE)
        .check();
  }

  @Test void testRightJoinWithConstantRowOnLeft() {
    final String sql = "select e.empno, e.ename, c.*"
        + " from (select 5 as nm) c right join emp e on e.empno = c.nm";

    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_LEFT_INSTANCE)
        .check();
  }

  @Test void testLeftJoinWithConstantRowOnRight() {
    final String sql = "select e.empno, e.ename, c.*"
        + " from emp e left join (select 5 as nm) c  on e.empno = c.nm";

    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_RIGHT_INSTANCE)
        .check();
  }

  @Test void testInnerJoinWithTimeStampSingleRowOnRight() {
    final String sql = "select e.empno, e.ename, c.t"
        + " from emp e inner join (select 7934 as ono, current_timestamp as t) c on e.empno=c.ono";
    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_RIGHT_PROJECT_INSTANCE)
        .check();
  }

  @Test void testLeftJoinWithTimeStampSingleRowOnRight() {
    final String sql = "select e.empno, e.ename, c.t "
        + "from emp e left join (select 7934 as ono, current_timestamp as t) c "
        + "  on e.empno = c.ono";
    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_RIGHT_PROJECT_INSTANCE)
        .check();
  }

  @Test void testCrossJoinWithTimeStampSingleRowOnRight() {
    final String sql = "select e.empno, e.ename, c.*"
        + " from emp e cross join (select 5, current_timestamp) c";
    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_RIGHT_PROJECT_INSTANCE)
        .check();
  }
  @Test void testInnerJoinWithTimeStampSingleRowOnLeft() {
    final String sql = "select e.empno, e.ename, c.t"
        + " from (select 7934 as ono, current_timestamp as t) c inner join emp e on e.empno=c.ono";
    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_LEFT_PROJECT_INSTANCE)
        .check();
  }

  @Test void testRightJoinWithTimeStampSingleRowOnLeft() {
    final String sql = "select e.empno, e.ename, c.t "
        + "from (select 7934 as ono, current_timestamp as t) c right join emp e "
        + "  on e.empno = c.ono";
    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_LEFT_PROJECT_INSTANCE)
        .check();
  }

  @Test void testCrossJoinWithTimeStampSingleRowOnLeft() {
    final String sql = "select e.empno, e.ename, c.*"
        + " from (select 5, current_timestamp) c cross join emp e ";
    sql(sql)
        .withRule(SingleValuesOptimizationRules.JOIN_LEFT_PROJECT_INSTANCE)
        .check();
  }

  @Test void testEmptyTableTransformsComplexQueryToSingleTableScan() {
    final String sql = "select products.PRODUCTID, products.NAME from products left join\n"
        + "(select * from products as e\n"
        + " inner join EMPTY_PRODUCTS as d on e.PRODUCTID = d.PRODUCTID\n"
        + " where e.SUPPLIERID > 10) dt\n"
        + " on products.PRODUCTID = dt.PRODUCTID";
    Collection<RelOptRule> rules =
        Arrays.asList(PruneEmptyRules.EMPTY_TABLE_INSTANCE,
            PruneEmptyRules.JOIN_RIGHT_INSTANCE,
            PruneEmptyRules.FILTER_INSTANCE,
            PruneEmptyRules.PROJECT_INSTANCE,
            CoreRules.PROJECT_MERGE);
    sql(sql)
        .withProgram(HepProgram.builder().addRuleCollection(rules).build())
        .check();
  }

  @Test void testLeftEmptyInnerJoin() {
    // Plan should be empty
    final String sql = "select * from (\n"
        + "select * from emp where false) as e\n"
        + "join dept as d on e.deptno = d.deptno";
    checkEmptyJoin(sql(sql));
  }

  @Test void testLeftEmptyLeftJoin() {
    // Plan should be empty
    final String sql = "select * from (\n"
        + "  select * from emp where false) e\n"
        + "left join dept d on e.deptno = d.deptno";
    checkEmptyJoin(sql(sql));
  }

  @Test void testLeftEmptyRightJoin() {
    // Plan should be equivalent to "select * from emp right join dept".
    // Cannot optimize away the join because of RIGHT.
    final String sql = "select * from (\n"
        + "  select * from emp where false) e\n"
        + "right join dept d on e.deptno = d.deptno";
    checkEmptyJoin(sql(sql));
  }

  @Test void testLeftEmptyFullJoin() {
    // Plan should be equivalent to "select * from emp full join dept".
    // Cannot optimize away the join because of FULL.
    final String sql = "select * from (\n"
        + "  select * from emp where false) e\n"
        + "full join dept d on e.deptno = d.deptno";
    checkEmptyJoin(sql(sql));
  }

  @Test void testLeftEmptySemiJoin() {
    checkLeftEmptySemiOrAntiJoin(JoinRelType.SEMI);
  }

  @Test void testLeftEmptyAntiJoin() {
    checkLeftEmptySemiOrAntiJoin(JoinRelType.ANTI);
  }

  private void checkLeftEmptySemiOrAntiJoin(JoinRelType type) {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP").empty()
        .scan("DEPT")
        .join(type, b
            .equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO")))
        .project(b.field("EMPNO"))
        .build();
    checkEmptyJoin(relFn(relFn));
  }

  @Test void testRightEmptyInnerJoin() {
    // Plan should be empty
    final String sql = "select * from emp e\n"
        + "join (select * from dept where false) as d\n"
        + "on e.deptno = d.deptno";
    checkEmptyJoin(sql(sql));
  }

  @Test void testRightEmptyLeftJoin() {
    // Plan should be equivalent to "select * from emp left join dept".
    // Cannot optimize away the join because of LEFT.
    final String sql = "select * from emp e\n"
        + "left join (select * from dept where false) as d\n"
        + "on e.deptno = d.deptno";
    checkEmptyJoin(sql(sql));
  }

  @Test void testRightEmptyRightJoin() {
    // Plan should be empty
    final String sql = "select * from emp e\n"
        + "right join (select * from dept where false) as d\n"
        + "on e.deptno = d.deptno";
    checkEmptyJoin(sql(sql));
  }

  @Test void testRightEmptyFullJoin() {
    // Plan should be equivalent to "select * from emp full join dept".
    // Cannot optimize away the join because of FULL.
    final String sql = "select * from emp e\n"
        + "full join (select * from dept where false) as d\n"
        + "on e.deptno = d.deptno";
    checkEmptyJoin(sql(sql));
  }

  @Test void testRightEmptySemiJoin() {
    checkRightEmptyAntiJoin(JoinRelType.SEMI);
  }

  @Test void testRightEmptyAntiJoin() {
    checkRightEmptyAntiJoin(JoinRelType.ANTI);
  }

  private void checkRightEmptyAntiJoin(JoinRelType type) {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT").empty()
        .join(type, b
            .equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO")))
        .project(b.field("EMPNO"))
        .build();
    checkEmptyJoin(relFn(relFn));
  }

  @Test void testRightEmptyAntiJoinNonEqui() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT").empty()
        .antiJoin(b
            .equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO")),
            b
                .equals(
                    b.field(2, 0, "SAL"),
                    b.literal(2000)))
        .project(b.field("EMPNO"))
        .build();
    checkEmptyJoin(relFn(relFn));
  }

  @Test void testCorrelateWithoutCorrelationToEmpty() {
    String sql = "select * from emp e where exists (select 1 from dept where empno=null)";
    List<RelOptRule> rules =
        Arrays.asList(CoreRules.FILTER_REDUCE_EXPRESSIONS,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.AGGREGATE_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE,
        PruneEmptyRules.CORRELATE_RIGHT_INSTANCE);
    sql(sql)
        .withExpand(false)
        .withPreRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withProgram(HepProgram.builder().addRuleCollection(rules).build())
        .check();
  }

  @Test void testCorrelateWithoutCorrelationToEmptyInVolcano() {
    String sql = "select * from emp e where exists (select 1 from dept where empno=null)";
    List<RelOptRule> rules =
        Arrays.asList(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.AGGREGATE_INSTANCE,
            PruneEmptyRules.JOIN_RIGHT_INSTANCE,
            PruneEmptyRules.CORRELATE_RIGHT_INSTANCE);
    sql(sql)
        .withPreRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withVolcanoPlanner(false, p -> {
          rules.forEach(p::addRule);
          p.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        })
        .withExpand(false)
        .check();
  }

  @Test void testInnerCorrelateWithLeftEmpty() {
    relFn(correlationWithEmpty(JoinRelType.INNER, true, false))
        .withRule(PruneEmptyRules.CORRELATE_LEFT_INSTANCE)
        .check();
  }

  @Test void testLeftCorrelateWithLeftEmpty() {
    relFn(correlationWithEmpty(JoinRelType.LEFT, true, false))
        .withRule(PruneEmptyRules.CORRELATE_LEFT_INSTANCE)
        .check();
  }

  @Test void testSemiCorrelateWithLeftEmpty() {
    relFn(correlationWithEmpty(JoinRelType.SEMI, true, false))
        .withRule(PruneEmptyRules.CORRELATE_LEFT_INSTANCE)
        .check();
  }

  @Test void testAntiCorrelateWithLeftEmpty() {
    relFn(correlationWithEmpty(JoinRelType.ANTI, true, false))
        .withRule(PruneEmptyRules.CORRELATE_LEFT_INSTANCE)
        .check();
  }

  @Test void testInnerCorrelateWithRightEmpty() {
    relFn(correlationWithEmpty(JoinRelType.INNER, false, true))
        .withRule(PruneEmptyRules.CORRELATE_RIGHT_INSTANCE)
        .check();
  }

  @Test void testLeftCorrelateWithRightEmpty() {
    relFn(correlationWithEmpty(JoinRelType.LEFT, false, true))
        .withRule(PruneEmptyRules.CORRELATE_RIGHT_INSTANCE)
        .check();
  }

  @Test void testSemiCorrelateWithRightEmpty() {
    relFn(correlationWithEmpty(JoinRelType.SEMI, false, true))
        .withRule(PruneEmptyRules.CORRELATE_RIGHT_INSTANCE)
        .check();
  }

  @Test void testAntiCorrelateWithRightEmpty() {
    relFn(correlationWithEmpty(JoinRelType.ANTI, false, true))
        .withRule(PruneEmptyRules.CORRELATE_RIGHT_INSTANCE)
        .check();
  }

  @Test void testInnerCorrelateWithNoneEmpty() {
    relFn(correlationWithEmpty(JoinRelType.INNER, false, false))
        .withRule(PruneEmptyRules.CORRELATE_RIGHT_INSTANCE)
        .withRule(PruneEmptyRules.CORRELATE_LEFT_INSTANCE)
        .checkUnchanged();
  }

  @Test void testLeftCorrelateWithBothEmpty1() {
    relFn(correlationWithEmpty(JoinRelType.LEFT, true, true))
        .withRule(PruneEmptyRules.CORRELATE_RIGHT_INSTANCE)
        .check();
  }

  @Test void testLeftCorrelateWithBothEmpty2() {
    relFn(correlationWithEmpty(JoinRelType.LEFT, true, true))
        .withRule(PruneEmptyRules.CORRELATE_LEFT_INSTANCE)
        .check();
  }

  @Test void testLeftCorrelateWithBothEmpty3() {
    List<RelOptRule> rules =
        Arrays.asList(PruneEmptyRules.CORRELATE_RIGHT_INSTANCE,
        PruneEmptyRules.CORRELATE_LEFT_INSTANCE);
    relFn(correlationWithEmpty(JoinRelType.LEFT, true, true))
        .withProgram(HepProgram.builder().addRuleCollection(rules).build())
        .check();
  }

  private static Function<RelBuilder, RelNode> correlationWithEmpty(JoinRelType joinType,
      boolean emptyLeft, boolean emptyRight) {
    final Holder<RexCorrelVariable> v = Holder.empty();
    return b -> {
      List<RexNode> requiredFields = new ArrayList<>();
      b.scan("EMP").variable(v::set);
      if (emptyLeft) {
        b.empty();
      }
      requiredFields.add(b.field("EMP", "DEPTNO"));
      b.scan("DEPT").filter(b.equals(b.field(v.get(), "DEPTNO"), b.field("DEPTNO")));
      if (emptyRight) {
        b.empty();
      }
      return b.correlate(joinType, v.get().id, requiredFields).build();
    };
  }

  private void checkEmptyJoin(RelOptFixture f) {
    f.withRule(
        CoreRules.FILTER_REDUCE_EXPRESSIONS,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.JOIN_LEFT_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE).check();
  }

  @Test void testEmptySort() {
    final String sql = "select * from emp where false order by deptno";
    sql(sql)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.SORT_INSTANCE)
        .check();
  }

  @Test void testEmptySort2() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("DEPT").empty()
        .sort(
            b.field("DNAME"),
            b.field("DEPTNO"))
        .build();
    relFn(relFn).withRule(PruneEmptyRules.SORT_INSTANCE).check();
  }

  @Test void testEmptySortLimitZero() {
    final String sql = "select * from emp order by deptno limit 0";
    sql(sql).withRule(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE).check();
  }

  @Test void testEmptyAggregate() {
    final String sql = "select sum(empno) from emp where false group by deptno";
    sql(sql)
        .withPreRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            PruneEmptyRules.PROJECT_INSTANCE)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.AGGREGATE_INSTANCE,
            PruneEmptyRules.PROJECT_INSTANCE)
        .check();
  }

  @Test void testEmptyAggregateEmptyKey() {
    final String sql = "select sum(empno) from emp where false";
    sql(sql)
        .withPreRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            PruneEmptyRules.PROJECT_INSTANCE)
        .withRule(PruneEmptyRules.AGGREGATE_INSTANCE)
        .checkUnchanged();
  }

  @Test void testEmptyAggregateEmptyKeyWithAggregateValuesRule() {
    final String sql = "select count(*), sum(empno) from emp where false";
    sql(sql)
        .withPreRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            PruneEmptyRules.PROJECT_INSTANCE)
        .withRule(CoreRules.AGGREGATE_VALUES)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5117">[CALCITE-5117]
   * Optimize the EXISTS sub-query by Metadata RowCount</a>. */
  @Test void testExistsWithAtLeastOneRowSubQuery() {
    final String sql = "select * from dept as d\n"
        + "where EXISTS (\n"
        + "  select count(*) from emp e where d.deptno = e.deptno)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testExistsWithNoRowSubQuery() {
    final String sql = "select * from dept as d\n"
        + "where NOT EXISTS (\n"
        + "  select count(*) from emp e having false)";
    sql(sql).withSubQueryRules().check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6650">[CALCITE-6650]
   * Optimize the IN sub-query and SOME sub-query
   * by Metadata RowCount</a>. */
  @Test void testInWithNoRowSubQuery() {
    final String sql = "select * from dept as d\n"
        + "where deptno in (\n"
        + "  select deptno from emp e where false)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testNotInWithNoRowSubQuery() {
    final String sql = "select * from dept as d\n"
        + "where deptno not in (\n"
        + "  select deptno from emp e where false)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testSomeWithGreaterThanNoRowSubQuery() {
    final String sql = "select * from dept as d\n"
        + "where deptno > some(\n"
        + "  select deptno from emp e where false)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testSomeWithLessThanOrEqualNoRowSubQuery() {
    final String sql = "select * from dept as d\n"
        + "where deptno <= some(\n"
        + "  select deptno from emp e where false)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testUniqueWithNoRowSubQuery() {
    final String sql = "select * from dept as d\n"
        + "where unique(\n"
        + "  select deptno from emp e where false)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testNotUniqueWithNoRowSubQuery() {
    final String sql = "select * from dept as d\n"
        + "where not unique(\n"
        + "  select deptno from emp e where false)";
    sql(sql).withSubQueryRules().check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4848">[CALCITE-4848]
   * Adding a HAVING condition to a query with a dynamic parameter makes the result always empty
   </a>. */
  @Test void testAggregateWithDynamicParam() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    final String sql = "SELECT sal, COUNT(1) AS count_val\n"
        + "FROM emp t WHERE sal = ?\n"
        + "GROUP BY sal HAVING sal < 1000";
    sql(sql).withPlanner(hepPlanner)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6647">[CALCITE-6647]
   * SortUnionTransposeRule should not push SORT past a UNION when SORT's fetch is DynamicParam
   </a>. */
  @Test void testSortWithDynamicParam() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(SortProjectTransposeRule.class);
    builder.addRuleClass(SortUnionTransposeRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.SORT_PROJECT_TRANSPOSE);
    hepPlanner.addRule(CoreRules.SORT_UNION_TRANSPOSE);
    final String sql = "SELECT x.sal\n"
        + "FROM (SELECT emp1.sal\n"
        + "      FROM (SELECT sal\n"
        + "            from emp\n"
        + "            LIMIT ?) AS emp1\n"
        + "      UNION ALL\n"
        + "      SELECT emp2.sal\n"
        + "      FROM (SELECT sal\n"
        + "            from emp\n"
        + "            LIMIT ?) AS emp2) AS x\n"
        + "LIMIT ?";
    sql(sql).withPlanner(hepPlanner).check();
  }

  @Test void testReduceCasts() {
    // Disable simplify in RelBuilder so that there are casts in 'before';
    // The resulting plan should have no cast expressions
    final String sql = "select cast(d.name as varchar(128)), cast(e.empno as integer)\n"
        + "from dept as d inner join emp as e\n"
        + "on cast(d.deptno as integer) = cast(e.deptno as integer)\n"
        + "where cast(e.job as varchar(1)) = 'Manager'";
    sql(sql)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .withRelBuilderSimplify(false)
        .check();
  }

  /** Tests that a cast from a TIME to a TIMESTAMP is not reduced. It is not
   * constant because the result depends upon the current date. */
  @Test void testReduceCastTimeUnchanged() {
    sql("select cast(time '12:34:56' as timestamp) from emp as e")
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  @Test void testReduceCastAndConsts() {
    // Make sure constant expressions inside the cast can be reduced
    // in addition to the casts.
    final String sql = "select * from emp\n"
        + "where cast((empno + (10/2)) as int) = 13";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS).check();
  }

  @Test void testReduceCaseNullabilityChange() {
    final String sql = "select case when empno = 1 then 1\n"
        + "when 1 IS NOT NULL then 2\n"
        + "else null end as qx "
        + "from emp";
    sql(sql)
        .withRelBuilderSimplify(false)
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testReduceCastsNullable() {
    HepProgram program = new HepProgramBuilder()

        // Simulate the way INSERT will insert casts to the target types
        .addRuleInstance(
            CoerceInputsRule.Config.DEFAULT
                .withCoerceNames(false)
                .withConsumerRelClass(LogicalTableModify.class)
                .toRule())

            // Convert projects to calcs, merge two calcs, and then
            // reduce redundant casts in merged calc.
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .addRuleInstance(CoreRules.CALC_MERGE)
        .addRuleInstance(CoreRules.CALC_REDUCE_EXPRESSIONS)
        .build();
    final String sql = "insert into sales.dept(deptno, name)\n"
        + "select empno, cast(job as varchar(128)) from sales.empnullables";
    sql(sql).withProgram(program).check();
  }

  @Test void testReduceCaseWhenWithCast() {
    final Function<RelBuilder, RelNode> relFn = b -> {
      final RexBuilder rexBuilder = b.getRexBuilder();
      final RelDataType type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

      RelNode left = b
          .values(new String[]{"x", "y"}, 1, 2, 2, 1).build();
      RexNode ref = rexBuilder.makeInputRef(left, 0);
      RexLiteral literal1 = rexBuilder.makeLiteral(1, type);
      RexLiteral literal2 = rexBuilder.makeLiteral(2, type);
      RexLiteral literal3 = rexBuilder.makeLiteral(3, type);

      // CASE WHEN x % 2 = 1 THEN x < 2
      //      WHEN x % 3 = 2 THEN x < 1
      //      ELSE x < 3
      final RexNode caseRexNode =
          rexBuilder.makeCall(
              SqlStdOperatorTable.CASE,
              rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                  rexBuilder.makeCall(SqlStdOperatorTable.MOD, ref, literal2),
                  literal1),
              rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal2),
              rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                  rexBuilder.makeCall(SqlStdOperatorTable.MOD, ref, literal3),
                  literal2),
              rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal1),
              rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ref, literal3));

      final RexNode castNode =
          rexBuilder.makeCast(
              rexBuilder.getTypeFactory()
                  .createTypeWithNullability(caseRexNode.getType(), true),
              caseRexNode);
      return b
          .push(left)
          .project(castNode)
          .build();
    };

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ReduceExpressionsRule.class);

    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS);

    relFn(relFn).withPlanner(hepPlanner).checkUnchanged();
  }

  private void basePushAggThroughUnion() {
    sql("${sql}")
        .withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.PROJECT_MERGE,
            CoreRules.AGGREGATE_UNION_TRANSPOSE)
        .check();
  }

  @Test void testPushSumConstantThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullConstantThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableNOGBYThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushCountStarThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushCountNullableThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushMaxNullableThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushMinThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushAvgThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushSumCountStarThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushSumConstantGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullConstantGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushSumNullableGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushCountStarGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushCountNullableGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushMaxNullableGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushMinGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushAvgGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushSumCountStarGroupingSetsThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushCountFilterThroughUnion() {
    basePushAggThroughUnion();
  }

  @Test void testPushBoolAndBoolOrThroughUnion() {
    sql("${sql}")
        .withFactory(f ->
            f.withOperatorTable(opTab ->
                SqlValidatorTest.operatorTableFor(SqlLibrary.POSTGRESQL)))
        .withRule(CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.PROJECT_MERGE,
            CoreRules.AGGREGATE_UNION_TRANSPOSE)
        .check();
  }

  @Test void testPullFilterThroughAggregate() {
    final String sql = "select ename, sal, deptno from ("
        + "  select ename, sal, deptno"
        + "  from emp"
        + "  where sal > 5000)"
        + "group by ename, sal, deptno";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.AGGREGATE_FILTER_TRANSPOSE)
        .check();
  }

  @Test void testPullFilterThroughAggregateGroupingSets() {
    final String sql = "select ename, sal, deptno from ("
        + "  select ename, sal, deptno"
        + "  from emp"
        + "  where sal > 5000)"
        + "group by rollup(ename, sal, deptno)";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.AGGREGATE_FILTER_TRANSPOSE)
        .check();
  }

  private void basePullConstantTroughAggregate() {
    sql("${sql}")
        .withRule(CoreRules.PROJECT_MERGE,
            CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testPullConstantThroughConstLast() {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateSimpleNonNullable() {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregatePermuted() {
    basePullConstantTroughAggregate();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6586">[CALCITE-6586]
   * Some Rules not firing due to RelMdPredicates returning null in VolcanoPlanner</a>. */
  @Test void testPullConstantThroughAggregatePermutedInVolcano() {
    sql("${sql}")
        .withVolcanoPlanner(false, p -> {
          p.addRule(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);
          p.addRule(CoreRules.PROJECT_MERGE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        })
        .check();
  }

  @Test void testPullConstantThroughAggregatePermutedConstFirst() {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregatePermutedConstGroupBy() {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateConstGroupBy() {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateAllConst() {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughAggregateAllLiterals() {
    basePullConstantTroughAggregate();
  }

  @Test void testPullConstantThroughUnion() {
    final String sql = "select 2, deptno, job from emp as e1\n"
        + "union all\n"
        + "select 2, deptno, job from emp as e2";
    sql(sql)
        .withTrim(true)
        .withRule(CoreRules.UNION_PULL_UP_CONSTANTS,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6586">[CALCITE-6586]
   * Some Rules not firing due to RelMdPredicates returning null in VolcanoPlanner</a>. */
  @Test void testPullConstantThroughUnionInVolcano() {
    final String sql = "select 2, deptno, job from emp as e1\n"
        + "union all\n"
        + "select 2, deptno, job from emp as e2";
    sql(sql)
        .withTrim(true)
        .withVolcanoPlanner(false, p -> {
          p.addRule(CoreRules.UNION_PULL_UP_CONSTANTS);
          p.addRule(CoreRules.PROJECT_MERGE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);
        })
        .check();
  }

  @Test void testPullConstantThroughUnion2() {
    // Negative test: constants should not be pulled up
    final String sql = "select 2, deptno, job from emp as e1\n"
        + "union all\n"
        + "select 1, deptno, job from emp as e2";
    sql(sql)
        .withRule(CoreRules.UNION_PULL_UP_CONSTANTS,
            CoreRules.PROJECT_MERGE)
        .checkUnchanged();
  }

  @Test void testPullConstantThroughUnion3() {
    // We should leave at least a single column in each Union input
    final String sql = "select 2, 3 from emp as e1\n"
        + "union all\n"
        + "select 2, 3 from emp as e2";
    sql(sql)
        .withTrim(true)
        .withRule(CoreRules.UNION_PULL_UP_CONSTANTS,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testPullConstantThroughUnionSameTypeNullableField() {
    final String sql = "select deptno, ename from empnullables where deptno = 1\n"
        + "union all\n"
        + "select deptno, ename from empnullables where deptno = 1";
    sql(sql)
        .withTrim(true)
        .withRule(CoreRules.UNION_PULL_UP_CONSTANTS,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testAggregateProjectMerge() {
    final String sql = "select x, sum(z), y from (\n"
        + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
        + "  from emp)\n"
        + "group by x, y";
    sql(sql).withRule(CoreRules.AGGREGATE_PROJECT_MERGE).check();
  }

  @Test void testAggregateGroupingSetsProjectMerge() {
    final String sql = "select x, sum(z), y from (\n"
        + "  select deptno as x, empno as y, sal as z, sal * 2 as zz\n"
        + "  from emp)\n"
        + "group by rollup(x, y)";
    sql(sql).withRule(CoreRules.AGGREGATE_PROJECT_MERGE).check();
  }

  @Test void testAggregateExtractProjectRule() {
    final String sql = "select sum(sal)\n"
        + "from emp";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    sql(sql).withPre(pre).withRule(AggregateExtractProjectRule.SCAN).check();
  }

  @Test void testAggregateExtractProjectRuleWithGroupingSets() {
    final String sql = "select empno, deptno, sum(sal)\n"
        + "from emp\n"
        + "group by grouping sets ((empno, deptno),(deptno),(empno))";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    sql(sql).withPre(pre).withRule(AggregateExtractProjectRule.SCAN).check();
  }

  /** Test with column used in both grouping set and argument to aggregate
   * function. */
  @Test void testAggregateExtractProjectRuleWithGroupingSets2() {
    final String sql = "select empno, deptno, sum(empno)\n"
        + "from emp\n"
        + "group by grouping sets ((empno, deptno),(deptno),(empno))";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    sql(sql).withPre(pre).withRule(AggregateExtractProjectRule.SCAN).check();
  }

  @Test void testAggregateExtractProjectRuleWithFilter() {
    final String sql = "select sum(sal) filter (where empno = 40)\n"
        + "from emp";
    HepProgram pre = new HepProgramBuilder()
        .addRuleInstance(CoreRules.AGGREGATE_PROJECT_MERGE)
        .build();
    // AggregateProjectMergeRule does not merges Project with Filter.
    // Force match Aggregate on top of Project once explicitly in unit test.
    final AggregateExtractProjectRule rule =
        AggregateExtractProjectRule.SCAN.config
            .withOperandSupplier(b0 ->
                b0.operand(Aggregate.class).oneInput(b1 ->
                    b1.operand(Project.class)
                        .predicate(new Predicate<Project>() {
                          int matchCount = 0;

                          public boolean test(Project project) {
                            return matchCount++ == 0;
                          }
                        }).anyInputs()))
            .as(AggregateExtractProjectRule.Config.class)
            .toRule();
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
        + " sum(case when deptno = 45 then 1 end) as count_d45,\n"
        + " sum(case when deptno = 50 then 1 else null end) as count_d50,\n"
        + " sum(case when deptno = 60 then null end) as sum_null_d60,\n"
        + " sum(case when deptno = 70 then null else 1 end) as sum_null_d70,\n"
        + " count(case when deptno = 20 then 1 end) as count_d20\n"
        + "from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_CASE_TO_FILTER).check();
  }

  @Test void testAggregateCaseToFilterNoMatch() {
    final String sql = "select\n"
        + " sum(case when deptno = -1 then 1 else 0 end) as sum_no_match,\n"
        + " sum(case when deptno = -1 then 2 else 0 end) as sum_no_match2,\n"
        + " sum(case when deptno = -1 then 3 else -1 end) as sum_no_match3\n"
        + "from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_CASE_TO_FILTER).checkUnchanged();
  }

  @Test void testPullAggregateThroughUnion() {
    final String sql = "select deptno, job from"
        + " (select deptno, job from emp as e1"
        + " group by deptno,job"
        + "  union all"
        + " select deptno, job from emp as e2"
        + " group by deptno,job)"
        + " group by deptno,job";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_UNION_AGGREGATE)
        .check();
  }

  @Test void testPullAggregateThroughUnion2() {
    final String sql = "select deptno, job from"
        + " (select deptno, job from emp as e1"
        + " group by deptno,job"
        + "  union all"
        + " select deptno, job from emp as e2"
        + " group by deptno,job)"
        + " group by deptno,job";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND,
            CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST)
        .check();
  }

  /**
   * Once the bottom aggregate pulled through union, we need to add a Project
   * if the new input contains a different type from the union.
   */
  @Test void testPullAggregateThroughUnionAndAddProjects() {
    final String sql = "select job, deptno from"
        + " (select job, deptno from emp as e1"
        + " group by job, deptno"
        + "  union all"
        + " select job, deptno from emp as e2"
        + " group by job, deptno)"
        + " group by job, deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_UNION_AGGREGATE)
        .check();
  }

  /**
   * Make sure the union alias is preserved when the bottom aggregate is
   * pulled up through union.
   */
  @Test void testPullAggregateThroughUnionWithAlias() {
    final String sql = "select job, c from"
        + " (select job, deptno c from emp as e1"
        + " group by job, deptno"
        + "  union all"
        + " select job, deptno from emp as e2"
        + " group by job, deptno)"
        + " group by job, c";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_UNION_AGGREGATE)
        .check();
  }

  /**
   * Creates a {@link HepProgram} with common transitive rules.
   */
  private HepProgram getTransitiveProgram() {
    return new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN_DUMB)
        .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_SET_OP_TRANSPOSE)
        .build();
  }

  @Test void testTransitiveInferenceJoin() {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno where e.deptno > 7";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceProject() {
    final String sql = "select 1 from (select * from sales.emp where deptno > 7) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceAggregate() {
    final String sql = "select 1 from (select deptno, count(*) from sales.emp where deptno > 7\n"
        + "group by deptno) d inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceUnion() {
    final String sql = "select 1 from\n"
        + "(select deptno from sales.emp where deptno > 7\n"
        + "union all select deptno from sales.emp where deptno > 10) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceJoin3way() {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "inner join sales.emp f on e.deptno = f.deptno\n"
        + "where d.deptno > 7";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceJoin3wayAgg() {
    final String sql = "select 1 from\n"
        + "(select deptno, count(*) from sales.emp where deptno > 7 group by deptno) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "inner join sales.emp f on e.deptno = f.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceLeftOuterJoin() {
    final String sql = "select 1 from sales.emp d\n"
        + "left outer join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno > 7 and e.deptno > 9";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceRightOuterJoin() {
    final String sql = "select 1 from sales.emp d\n"
        + "right outer join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno > 7 and e.deptno > 9";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceFullOuterJoin() {
    final String sql = "select 1 from sales.emp d full outer join sales.emp e\n"
        + "on d.deptno = e.deptno  where d.deptno > 7 and e.deptno > 9";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).checkUnchanged();
  }

  @Test void testTransitiveInferencePreventProjectPullUp() {
    final String sql = "select 1 from (select comm as deptno from sales.emp where deptno > 7) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).checkUnchanged();
  }

  @Test void testTransitiveInferencePullUpThruAlias() {
    final String sql = "select 1 from (select comm as deptno from sales.emp where comm > 7) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceConjunctInPullUp() {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno in (7, 9) or d.deptno > 10";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceNoPullUpExprs() {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno in (7, 9) or d.comm > 10";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).checkUnchanged();
  }

  @Test void testTransitiveInferenceUnion3way() {
    final String sql = "select 1 from\n"
        + "(select deptno from sales.emp where deptno > 7\n"
        + "union all\n"
        + "select deptno from sales.emp where deptno > 10\n"
        + "union all\n"
        + "select deptno from sales.emp where deptno > 1) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceUnion3wayOr() {
    final String sql = "select 1 from\n"
        + "(select empno, deptno from sales.emp where deptno > 7 or empno < 10\n"
        + "union all\n"
        + "select empno, deptno from sales.emp where deptno > 10 or empno < deptno\n"
        + "union all\n"
        + "select empno, deptno from sales.emp where deptno > 1) d\n"
        + "inner join sales.emp e on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-443">[CALCITE-443]
   * getPredicates from a union is not correct</a>. */
  @Test void testTransitiveInferenceUnionAlwaysTrue() {
    final String sql = "select d.deptno, e.deptno from\n"
        + "(select deptno from sales.emp where deptno < 4) d\n"
        + "inner join\n"
        + "(select deptno from sales.emp where deptno > 7\n"
        + "union all select deptno from sales.emp) e\n"
        + "on d.deptno = e.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testTransitiveInferenceConstantEquiPredicate() {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno  where 1 = 1";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).checkUnchanged();
  }

  @Test void testTransitiveInferenceComplexPredicate() {
    final String sql = "select 1 from sales.emp d\n"
        + "inner join sales.emp e on d.deptno = e.deptno\n"
        + "where d.deptno > 7 and e.sal = e.deptno and d.comm = d.deptno\n"
        + "and d.comm + d.deptno > d.comm/2";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  @Test void testPullConstantIntoProject() {
    final String sql = "select deptno, deptno + 1, empno + deptno\n"
        + "from sales.emp\n"
        + "where deptno = 10";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Tests propagation of a filter derived from an "IS NOT DISTINCT FROM"
   * predicate.
   *
   * <p>By the time the rule sees the predicate, it has been converted to
   * "deptno = 10 OR (deptno IS NULL AND 10 IS NULL)", so
   * {@link #testPullConstantIntoProjectWithIsNotDistinctFromDate()} is a purer
   * test of the functionality. */
  @Test void testPullConstantIntoProjectWithIsNotDistinctFrom() {
    final String sql = "select deptno, deptno + 1, empno + deptno\n"
        + "from sales.emp\n"
        + "where deptno is not distinct from 10";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testPullConstantIntoProjectWithIsNotDistinctFromForNull() {
    final String sql = "select mgr, deptno\n"
        + "from sales.emp\n"
        + "where mgr is not distinct from null";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5336">[CALCITE-5336]
   * Infer constants from predicates with IS NOT DISTINCT FROM operator</a>.
   *
   * <p>Reduce expression rules should identify constants with
   * {@code IS NOT DISTINCT FROM} operator as they do for {@code =}. */
  @Test void testPullConstantIntoProjectWithIsNotDistinctFromDate() {
    // Build a tree equivalent to the SQL
    //   SELECT hiredate
    //   FROM emp
    //   WHERE hiredate IS NOT DISTINCT FROM DATE '2020-12-11'
    //
    // We build a tree because if we used the SQL parser, it would expand
    //   x IS NOT DISTINCT FROM y
    // to
    //   x = y OR (x IS NULL AND y IS NULL)
    final Function<RelBuilder, RelNode> relFn = b ->
        b.scan("EMP")
            .filter(
                b.getRexBuilder()
                    .makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                        b.field(4),
                        b.literal(new DateString(2020, 12, 11))))
            .project(b.field(4))
            .build();

    relFn(relFn)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testPullConstantIntoFilter() {
    final String sql = "select * from (select * from sales.emp where deptno = 10)\n"
        + "where deptno + 5 > empno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testPullUpPredicatesFromUnionWithProject() {
    final String sql = "select empno = null from emp where comm = 2\n"
        + "union all\n"
        + "select comm = 2 from emp where comm = 2";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS)
          .check();
  }

  @Test void testPullUpPredicatesFromProject1() {
    final String sql = "select comm <> 2, comm = 2 from emp where comm = 2";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testPullUpPredicatesFromProject2() {
    final String sql = "select comm = 2, empno <> 1 from emp where comm = 2 and empno = 1";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1995">[CALCITE-1995]
   * Remove predicates from Filter if they can be proved to be always true or
   * false</a>. */
  @Test void testSimplifyFilter() {
    final String sql = "select * from (select * from sales.emp where deptno > 10)\n"
        + "where empno > 3 and deptno > 5";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testPullConstantIntoJoin() {
    final String sql = "select * from (select * from sales.emp where empno = 10) as e\n"
        + "left join sales.dept as d on e.empno = d.deptno";
    sql(sql).withPre(getTransitiveProgram())
        .withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .check();
  }

  @Test void testPullConstantIntoJoin2() {
    final String sql = "select * from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.empno = d.deptno and e.deptno + e.empno = d.deptno + 5";
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
        .addRuleCollection(
            ImmutableList.of(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
                CoreRules.FILTER_PROJECT_TRANSPOSE,
                CoreRules.JOIN_REDUCE_EXPRESSIONS))
        .build();
    sql(sql).withPre(getTransitiveProgram()).withProgram(program).check();
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
    sql(sql)
        .withDecorrelate(true)
        .withTrim(true)
        .withExpand(true)
        .withPreRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3111">[CALCITE-3111]
   * Allow custom implementations of Correlate in RelDecorrelator</a>. */
  @Test void testCustomDecorrelate() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)";

    // Convert sql to rel
    final RelOptFixture fixture = sql(sql).withExpand(true);
    final RelNode rel = fixture.toRel();

    // Create a duplicate rel tree with a CustomCorrelate instead of
    // LogicalCorrelate.
    final LogicalCorrelate logicalCorrelate =
        (LogicalCorrelate) rel.getInput(0).getInput(0);
    CustomCorrelate customCorrelate =
        new CustomCorrelate(logicalCorrelate.getCluster(),
            logicalCorrelate.getTraitSet(),
            logicalCorrelate.getHints(),
            logicalCorrelate.getLeft(),
            logicalCorrelate.getRight(),
            logicalCorrelate.getCorrelationId(),
            logicalCorrelate.getRequiredColumns(),
            logicalCorrelate.getJoinType());
    RelNode newRoot =
        rel.copy(rel.getTraitSet(),
            ImmutableList.of(
                rel.getInput(0).copy(rel.getInput(0).getTraitSet(),
                    ImmutableList.of(customCorrelate))));

    // Decorrelate both trees using the same relBuilder
    final RelBuilder relBuilder = RelBuilder.create(RelBuilderTest.config().build());
    RelNode logicalDecorrelated = RelDecorrelator.decorrelateQuery(rel, relBuilder);
    RelNode customDecorrelated = RelDecorrelator.decorrelateQuery(newRoot, relBuilder);
    String logicalDecorrelatedPlan = NL + RelOptUtil.toString(logicalDecorrelated);
    String customDecorrelatedPlan = NL + RelOptUtil.toString(customDecorrelated);

    // Ensure that the plans are equal
    fixture.diffRepos.assertEquals(
        "Comparing Plans from LogicalCorrelate and CustomCorrelate",
        logicalDecorrelatedPlan, customDecorrelatedPlan);
  }

  @Test void testProjectWindowTransposeRule() {
    final String sql = "select count(empno) over(), deptno from emp";
    sql(sql)
        .withRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
            CoreRules.PROJECT_WINDOW_TRANSPOSE)
        .check();
  }

  @Test void testProjectWindowTransposeRuleWithConstants() {
    final String sql = "select col1, col2\n"
        + "from (\n"
        + "  select empno,\n"
        + "    sum(100) over (partition by  deptno order by sal) as col1,\n"
        + "  sum(1000) over(partition by deptno order by sal) as col2\n"
        + "  from emp)";
    sql(sql)
        .withRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_WINDOW_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6632">[CALCITE-6632]
   * Wrong optimization because window missing constants in digest</a>. */
  @Test void testWindowMissingConstantInDigest() {
    final String sql = "select sum(100) over (partition by deptno order by sal) as s\n"
        + "from emp\n"
        + "union all\n"
        + "select sum(1000) over(partition by deptno order by sal) as s\n"
        + "from emp\n";
    sql(sql)
        .withRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
        .check();
  }

  /** While it's probably valid relational algebra for a Project to contain
   * a RexOver inside a RexOver, ProjectMergeRule should not bring it about. */
  @Test void testProjectMergeShouldIgnoreOver() {
    final String sql = "select row_number() over (order by deptno), col1\n"
        + "from (\n"
        + "  select deptno,\n"
        + "    sum(100) over (partition by  deptno order by sal) as col1\n"
        + "  from emp)";
    sql(sql).withRule(CoreRules.PROJECT_MERGE).checkUnchanged();
  }

  @Test void testAggregateProjectPullUpConstants() {
    final String sql = "select job, empno, sal, sum(sal) as s\n"
        + "from emp where empno = 10\n"
        + "group by job, empno, sal";
    sql(sql).withRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS).check();
  }

  @Test void testAggregateProjectPullUpConstants2() {
    final String sql = "select ename, sal\n"
        + "from (select '1', ename, sal from emp where ename = 'John') subq\n"
        + "group by ename, sal";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS)
        .check();
  }

  @Test void testPushFilterWithRank() {
    final String sql = "select e1.ename, r\n"
        + "from (\n"
        + "  select ename, "
        + "  rank() over(partition by  deptno order by sal) as r "
        + "  from emp) e1\n"
        + "where r < 2";
    sql(sql).withRule(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .checkUnchanged();
  }

  @Test void testPushFilterWithRankExpr() {
    final String sql = "select e1.ename, r\n"
        + "from (\n"
        + "  select ename,\n"
        + "  rank() over(partition by  deptno order by sal) + 1 as r "
        + "  from emp) e1\n"
        + "where r < 2";
    sql(sql).withRule(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-841">[CALCITE-841]
   * Redundant windows when window function arguments are expressions</a>. */
  @Test void testExpressionInWindowFunction() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectToWindowRule.class);

    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    final String sql = "select\n"
        + " sum(deptno) over(partition by deptno order by sal) as sum1,\n"
        + "sum(deptno + sal) over(partition by deptno order by sal) as sum2\n"
        + "from emp";
    sql(sql)
        .withPlanner(hepPlanner)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-888">[CALCITE-888]
   * Overlay window loses PARTITION BY list</a>. */
  @Test void testWindowInParenthesis() {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleClass(ProjectToWindowRule.class);
    HepPlanner hepPlanner = new HepPlanner(builder.build());
    hepPlanner.addRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    final String sql = "select count(*) over (w), count(*) over w\n"
        + "from emp\n"
        + "window w as (partition by empno order by empno)";
    sql(sql)
        .withPlanner(hepPlanner)
        .check();
  }

  /** Test case for DX-11490:
   * Make sure the planner doesn't fail over wrong push down
   * of is null. */
  @Test void testIsNullPushDown() {
    HepProgramBuilder preBuilder = new HepProgramBuilder();
    preBuilder.addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
    builder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    HepPlanner hepPlanner = new HepPlanner(builder.build());

    final String sql = "select empno, deptno, w_count from (\n"
        + "  select empno, deptno, count(empno) over (w) w_count\n"
        + "  from emp\n"
        + "  window w as (partition by deptno order by empno)\n"
        + ") sub_query where w_count is null";
    sql(sql)
        .withPre(preBuilder.build())
        .withPlanner(hepPlanner)
        .check();
  }

  @Test void testIsNullPushDown2() {
    HepProgramBuilder preBuilder = new HepProgramBuilder();
    preBuilder.addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
    builder.addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    HepPlanner hepPlanner = new HepPlanner(builder.build());

    final String sql = "select empno, deptno, w_count from (\n"
        + "  select empno, deptno, count(empno) over (ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) w_count\n"
        + "  from emp\n"
        + ") sub_query where w_count is null";
    sql(sql)
        .withPre(preBuilder.build())
        .withPlanner(hepPlanner)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5861">
   * [CALCITE-5861] ReduceExpressionsRule rules should constant-fold
   * expressions in window bounds</a>.
   */
  @Test void testExpressionPreceding() {
    HepProgramBuilder preBuilder = new HepProgramBuilder();
    preBuilder.addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
    preBuilder.addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

    final String sql =
        "select COUNT(*) over (\n"
            + "ORDER BY empno\n"
            + "ROWS BETWEEN 5 + 5 PRECEDING AND 1 PRECEDING) AS w_count from emp\n";
    sql(sql)
        .withPre(preBuilder.build())
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .checkUnchanged();
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
    sql(sql).withRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW).check();
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
    sql(sql).withRule(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW).check();
  }

  @Test void testMeasureSort() {
    final String sql = "select deptno, c1\n"
        + "from (select deptno, job, count(*) + 1 as measure c1\n"
        + "  from emp)\n"
        + "order by deptno";
    sql(sql)
        .withRule(MeasureRules.PROJECT_SORT)
        .check();
  }

  @Test void testMeasureAggregate() {
    final String sql = "select deptno, c1\n"
        + "from (select deptno, job, count(*) + 1 as measure c1\n"
        + "  from emp)\n"
        + "group by deptno";
    sql(sql)
        .withRule(MeasureRules.AGGREGATE,
            CoreRules.PROJECT_MERGE,
            MeasureRules.PROJECT)
        .check();
  }

  /** Tests use of a measure in a non-aggregate query. Calls
   * {@link RelOptFixture#checkUnchanged()} because Sql-to-rel has done the
   * necessary work already. */
  @Test void testMeasureWithoutGroupBy() {
    final String sql = "with empm as\n"
        + "  (select *, avg(sal) as measure avgSal from emp)\n"
        + "select deptno, avgSal\n"
        + "from empm";
    sql(sql)
        .withRule(MeasureRules.AGGREGATE2,
            CoreRules.PROJECT_MERGE,
            MeasureRules.PROJECT)
        .checkUnchanged();
  }

  @Test void testMeasureWithoutGroupByWithOrderBy() {
    final String sql = "with empm as\n"
        + "  (select *, avg(sal) as measure avgSal from emp)\n"
        + "select deptno, avgSal\n"
        + "from empm\n"
        + "order by 2 desc limit 3";
    sql(sql)
        .withRule(MeasureRules.PROJECT_SORT,
            CoreRules.PROJECT_MERGE,
            MeasureRules.PROJECT)
        .check();
  }

  @Disabled
  @Test void testMeasureJoin() {
    final String sql = "with deptm as\n"
        + "  (select deptno, name, avg(char_length(name)) as measure m\n"
        + "   from dept)\n"
        + "select deptno, aggregate(m) as m\n"
        + "from deptm join emp using (deptno)\n"
        + "group by deptno";
    sql(sql)
        .withFactory(t ->
            t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.STANDARD, SqlLibrary.CALCITE))) // for AGGREGATE
        .withRule(MeasureRules.AGGREGATE,
            CoreRules.PROJECT_MERGE,
            MeasureRules.PROJECT)
        .check();
  }

  @Test void testPushAggregateThroughJoin1() {
    final String sql = "select e.job,d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by on non-join keys, group by on
   * non-null generating side only. */
  @Test void testPushAggregateThroughOuterJoin1() {
    final String sql = "select e.ename\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "group by e.ename";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by on non-join keys, on null
   * generating side only. */
  @Test void testPushAggregateThroughOuterJoin2() {
    final String sql = "select d.ename\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.emp as d on e.job = d.job\n"
        + "group by d.ename";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by on both side on non-join
   * keys. */
  @Test void testPushAggregateThroughOuterJoin3() {
    final String sql = "select e.ename, d.mgr\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.emp as d on e.job = d.job\n"
        + "group by e.ename,d.mgr";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by on key same as join key,
   * group by on non-null generating side. */
  @Test void testPushAggregateThroughOuterJoin4() {
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "group by e.job";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by on key same as join key,
   * group by on null generating side. */
  @Test void testPushAggregateThroughOuterJoin5() {
    final String sql = "select d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "group by d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by on key same as join key,
   * group by on both side. */
  @Test void testPushAggregateThroughOuterJoin6() {
    final String sql = "select e.job,d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by key is susbset of join keys,
   * group by on non-null generating side. */
  @Test void testPushAggregateThroughOuterJoin7() {
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by key is a subset of join keys,
   * group by on null generating side. */
  @Test void testPushAggregateThroughOuterJoin8() {
    final String sql = "select d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, group by key is susbset of join keys,
   * group by on both sides. */
  @Test void testPushAggregateThroughOuterJoin9() {
    final String sql = "select e.job, d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "left outer join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job, d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for outer join, with aggregate functions. */
  @Test void testPushAggregateThroughOuterJoin10() {
    final String sql = "select count(e.ename)\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "left outer join sales.emp as d on e.job = d.job\n"
        + "group by e.ename,d.mgr";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .checkUnchanged();
  }

  /** Test case for non-equi outer join. */
  @Test void testPushAggregateThroughOuterJoin11() {
    final String sql = "select e.empno,d.deptno\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "left outer join sales.dept as d on e.empno < d.deptno\n"
        + "group by e.empno,d.deptno";
    sql(sql)
        .withRelBuilderConfig(b -> b.withAggregateUnique(true))
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5879">
   * AssertionError during constant reduction of SPLIT expression that returns NULL</a>. */
  @Test public void testSplitNull() {
    final String query = "select split('1|2|3', NULL)";
    sql(query)
        .withFactory(
            t -> t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.BIG_QUERY))) // needed for SPLIT function
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5879">
   * AssertionError during constant reduction of SPLIT expression that returns NULL</a>. */
  @Test public void testSplitNull1() {
    final String query = "select split(NULL, '|')";
    sql(query)
        .withFactory(
            t -> t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.BIG_QUERY))) // needed for SPLIT function
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5879">
   * AssertionError during constant reduction of SPLIT expression that returns NULL</a>. */
  @Test public void testSplitNull2() {
    final String query = "select split(NULL, NULL)";
    sql(query)
        .withFactory(
            t -> t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.BIG_QUERY))) // needed for SPLIT function
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5882">
   * [CALCITE-5882] Compile-time evaluation of SPLIT function returns incorrect result</a>. */
  @Test public void testSplit() {
    final String query = "select split('1|2|3', '|')";
    sql(query)
        .withFactory(
            t -> t.withOperatorTable(opTab ->
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                    SqlLibrary.BIG_QUERY))) // needed for SPLIT function
        .withRule(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for right outer join, group by on key same as join
   * key, group by on (left)null generating side. */
  @Test void testPushAggregateThroughOuterJoin12() {
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "right outer join sales.dept as d on e.job = d.name\n"
        + "group by e.job";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for full outer join, group by on key same as join key,
   * group by on one side. */
  @Test void testPushAggregateThroughOuterJoin13() {
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "full outer join sales.dept as d on e.job = d.name\n"
        + "group by e.job";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for full outer join, group by on key same as join key,
   * group by on both side. */
  @Test void testPushAggregateThroughOuterJoin14() {
    final String sql = "select e.mgr, d.mgr\n"
        + "from sales.emp as e\n"
        + "full outer join sales.emp as d on e.mgr = d.mgr\n"
        + "group by d.mgr, e.mgr";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for full outer join, group by on both side on non-join
   * keys. */
  @Test void testPushAggregateThroughOuterJoin15() {
    final String sql = "select e.ename, d.mgr\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "full outer join sales.emp as d on e.job = d.job\n"
        + "group by e.ename,d.mgr";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for full outer join, group by key is susbset of join
   * keys. */
  @Test void testPushAggregateThroughOuterJoin16() {
    final String sql = "select e.job\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "full outer join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  @Test void testPushAggregateThroughJoin2() {
    final String sql = "select e.job,d.name\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "and e.deptno + e.empno = d.deptno + 5\n"
        + "group by e.job,d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  @Test void testPushAggregateThroughJoin3() {
    final String sql = "select e.empno,d.deptno\n"
        + "from (select * from sales.emp where empno = 10) as e\n"
        + "join sales.dept as d on e.empno < d.deptno\n"
        + "group by e.empno,d.deptno";
    sql(sql)
        .withRelBuilderConfig(b -> b.withAggregateUnique(true))
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1544">[CALCITE-1544]
   * AggregateJoinTransposeRule fails to preserve row type</a>. */
  @Test void testPushAggregateThroughJoin4() {
    final String sql = "select e.deptno\n"
        + "from sales.emp as e join sales.dept as d on e.deptno = d.deptno\n"
        + "group by e.deptno";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  @Test void testPushAggregateThroughJoin5() {
    final String sql = "select e.deptno, d.deptno\n"
        + "from sales.emp as e join sales.dept as d on e.deptno = d.deptno\n"
        + "group by e.deptno, d.deptno";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2200">[CALCITE-2200]
   * Infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test void testJoinPushTransitivePredicatesRule() {
    final String sql = "select d.deptno from sales.emp d where d.deptno\n"
        + "IN (select e.deptno from sales.emp e "
        + "where e.deptno = d.deptno or e.deptno = 4)";
    sql(sql)
        .withExpand(true)
        .withPreRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
        .withRule() // empty program
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2205">[CALCITE-2205]
   * One more infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test void testJoinPushTransitivePredicatesRule2() {
    final String sql = "select n1.SAL\n"
        + "from EMPNULLABLES_20 n1\n"
        + "where n1.SAL IN (\n"
        + "  select n2.SAL\n"
        + "  from EMPNULLABLES_20 n2\n"
        + "  where n1.SAL = n2.SAL or n1.SAL = 4)";
    sql(sql).withDecorrelate(true)
        .withExpand(true)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5387">[CALCITE-5387]
   * Type-mismatch on nullability in JoinPushTransitivePredicatesRule RelRule</a>. */
  @Test void testJoinPushTransitivePredicatesNullabilityIssue() {
    final String sql = "WITH\n"
        + "non_null_table AS (\n"
        + "  SELECT DATE '2023-08-07' AS date_col_non_null FROM dept\n"
        + "),\n"
        + "null_table AS (\n"
        + "  SELECT CAST(null as DATE) AS date_col_null FROM dept\n"
        + ")\n"
        + "SELECT *\n"
        + "FROM non_null_table\n"
        + "JOIN null_table\n"
        + "ON null_table.date_col_null = non_null_table.date_col_non_null";

    sql(sql).withRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES).check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6432">[CALCITE-6432]
   * Infinite loop for JoinPushTransitivePredicatesRule</a>. */
  @Test void testProjectPredicatePull() {
    final String sql = "select e.ename, d.dname\n"
        + "from (select ename, deptno from emp where deptno = 10) e\n"
        + "join (select name dname, deptno, * from dept) d\n"
        + "on e.deptno = d.deptno";
    final HepProgram program = new HepProgramBuilder()
        .addRuleCollection(
            ImmutableList.of(CoreRules.FILTER_PROJECT_TRANSPOSE,
                CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES))
        .build();
    sql(sql).withProgram(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2275">[CALCITE-2275]
   * JoinPushTransitivePredicatesRule wrongly pushes down NOT condition</a>. */
  @Test void testInferringPredicatesWithNotOperatorInJoinCondition() {
    final String sql = "select * from sales.emp d\n"
        + "join sales.emp e on e.deptno = d.deptno and d.deptno not in (4, 6)";
    sql(sql)
        .withRelBuilderSimplify(false)
        .withDecorrelate(true)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2195">[CALCITE-2195]
   * AggregateJoinTransposeRule fails to aggregate over unique column</a>. */
  @Test void testPushAggregateThroughJoin6() {
    final String sql = "select sum(B.sal)\n"
        + "from sales.emp as A\n"
        + "join (select distinct sal from sales.emp) as B\n"
        + "on A.sal=B.sal\n";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  @Test void testPushAggregateThroughJoin7() {
    final String sql = "select any_value(distinct B.sal)\n"
        + "from sales.emp as A\n"
        + "join (select distinct sal from sales.emp) as B\n"
        + "on A.sal=B.sal\n";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  @Test void testPushAggregateThroughJoin8() {
    final String sql = "select single_value(distinct B.sal)\n"
        + "from sales.emp as A\n"
        + "join (select distinct sal from sales.emp) as B\n"
        + "on A.sal=B.sal\n";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2278">[CALCITE-2278]
   * AggregateJoinTransposeRule fails to split aggregate call if input contains
   * an aggregate call and has distinct rows</a>. */
  @Test void testPushAggregateThroughJoinWithUniqueInput() {
    final String sql = "select A.job, B.mgr, A.deptno,\n"
        + "max(B.hiredate1) as hiredate1, sum(B.comm1) as comm1\n"
        + "from sales.emp as A\n"
        + "join (select mgr, sal, max(hiredate) as hiredate1,\n"
        + "    sum(comm) as comm1 from sales.emp group by mgr, sal) as B\n"
        + "on A.sal=B.sal\n"
        + "group by A.job, B.mgr, A.deptno";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** SUM is the easiest aggregate function to split. */
  @Test void testPushAggregateSumThroughJoin() {
    final String sql = "select e.job,sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2105">[CALCITE-2105]
   * AggregateJoinTransposeRule incorrectly makes a SUM NOT NULL when Aggregate
   * has no group keys</a>. */
  @Test void testPushAggregateSumWithoutGroupKeyThroughJoin() {
    final String sql = "select sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
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
    final String sql = "select sum(sal)\n"
        + "from (select * from sales.emp where ename = 'A') as e\n"
        + "join sales.dept as d on e.job = d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
            CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Push a variety of aggregate functions. */
  @Test void testPushAggregateFunctionsThroughJoin() {
    final String sql = "select e.job,\n"
        + "  min(sal) as min_sal, min(e.deptno) as min_deptno,\n"
        + "  sum(sal) + 1 as sum_sal_plus, max(sal) as max_sal,\n"
        + "  sum(sal) as sum_sal_2, count(sal) as count_sal,\n"
        + "  count(mgr) as count_mgr\n"
        + "from sales.emp as e\n"
        + "join sales.dept as d on e.job = d.name\n"
        + "group by e.job,d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Push a aggregate functions into a relation that is unique on the join
   * key. */
  @Test void testPushAggregateThroughJoinDistinct() {
    final String sql = "select d.name,\n"
        + "  sum(sal) as sum_sal, count(*) as c\n"
        + "from sales.emp as e\n"
        + "join (select distinct name from sales.dept) as d\n"
        + "  on e.job = d.name\n"
        + "group by d.name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6600">[CALCITE-6600]
   * AggregateJoinTransposeRule can throw ArrayIndexOutOfBoundsException when applied
   * on a SemiJoin</a>.*/
  @Test void testPushAggregateThroughSemiJoin() {
    final String sql = "select distinct sal\n"
        + "from (select * from sales.emp e where e.job in\n"
        + "(select d.name from sales.dept d))";
    sql(sql)
        .withLateDecorrelate(true)
        .withTrim(true)
        .withPreRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withRule(CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_TO_SEMI_JOIN,
            CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Push count(*) through join, no GROUP BY. */
  @Test void testPushAggregateSumNoGroup() {
    final String sql =
        "select count(*) from sales.emp join sales.dept on job = name";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3076">[CALCITE-3076]
   * AggregateJoinTransposeRule throws error for unique under aggregate keys when
   * generating merged calls</a>.*/
  @Test void testPushAggregateThroughJoinOnEmptyLogicalValues() {
    final String sql = "select count(*) volume, sum(C1.sal) C1_sum_sal "
        + "from (select sal, ename from sales.emp where 1=2) C1 "
        + "inner join (select ename from sales.emp) C2   "
        + "on C1.ename = C2.ename ";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2249">[CALCITE-2249]
   * AggregateJoinTransposeRule generates non-equivalent nodes if Aggregate
   * contains DISTINCT aggregate function</a>. */
  @Test void testPushDistinctAggregateIntoJoin() {
    final String sql = "select count(distinct sal) from sales.emp\n"
        + " join sales.dept on job = name";
    sql(sql).withRule(CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED)
        .checkUnchanged();
  }

  /** Tests that ProjectAggregateMergeRule removes unused aggregate calls but
   * not group keys. */
  @Test void testProjectAggregateMerge() {
    final String sql = "select deptno + ss\n"
        + "from (\n"
        + "  select job, deptno, min(sal) as ms, sum(sal) as ss\n"
        + "  from sales.emp\n"
        + "  group by job, deptno)";
    sql(sql).withRule(CoreRules.PROJECT_AGGREGATE_MERGE)
        .check();
  }

  /** Tests that ProjectAggregateMergeRule does nothing when all aggregate calls
   * are referenced. */
  @Test void testProjectAggregateMergeNoOp() {
    final String sql = "select deptno + ss + ms\n"
        + "from (\n"
        + "  select job, deptno, min(sal) as ms, sum(sal) as ss\n"
        + "  from sales.emp\n"
        + "  group by job, deptno)";
    sql(sql).withRule(CoreRules.PROJECT_AGGREGATE_MERGE)
        .checkUnchanged();
  }

  /** Tests that ProjectAggregateMergeRule converts {@code COALESCE(SUM(x), 0)}
   * into {@code SUM0(x)}. */
  @Test void testProjectAggregateMergeSum0() {
    final String sql = "select coalesce(sum_sal, 0) as ss0\n"
        + "from (\n"
        + "  select sum(sal) as sum_sal\n"
        + "  from sales.emp)";
    sql(sql).withRule(CoreRules.PROJECT_AGGREGATE_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6834">[CALCITE-6834]
   * In query that applies COALESCE to nullable SUM, EnumerableProjectToCalcRule
   * throws AssertionError</a>. */
  @Test void testProjectAggregateMergeSum01() {
    final String sql = "select coalesce(sum(cast(mgr as tinyint)), 0) as ss0\n"
        + "from sales.emp";
    sql(sql).withRule(CoreRules.PROJECT_AGGREGATE_MERGE)
        .check();
  }

  /** As {@link #testProjectAggregateMergeSum0()} but there is another use of
   * {@code SUM} that cannot be converted to {@code SUM0}. */
  @Test void testProjectAggregateMergeSum0AndSum() {
    final String sql = "select sum_sal * 2, coalesce(sum_sal, 0) as ss0\n"
        + "from (\n"
        + "  select sum(sal) as sum_sal\n"
        + "  from sales.emp)";
    sql(sql).withRule(CoreRules.PROJECT_AGGREGATE_MERGE)
        .check();
  }

  /** Tests that ProjectAggregateMergeRule does nothing with non-numeric literals
   * and does not throw an exception. */
  @Test void testProjectAggregateMergeNonNumericLiteral() {
    // Requires a NULLABLE column to trigger
    final SqlTestFactory.CatalogReaderFactory catalogReaderFactory = (typeFactory, caseSensitive) ->
        new MockCatalogReader(typeFactory, caseSensitive) {
          @Override public MockCatalogReader init() {
            MockSchema schema = new MockSchema("SALES");
            registerSchema(schema);
            final boolean nullable = true;
            final RelDataType timestampType =
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
                    nullable);
            String tableName = "NULLABLE";
            MockTable table = MockTable
                .create(this, schema, tableName, false, 100);
            table.addColumn("HIREDATE", timestampType);
            registerTable(table);
            return this;
          }
        }.init();
    final String sql = "select hiredate, coalesce(hiredate, {ts '1969-12-31 00:00:00'}) as c1\n"
        + "from sales.nullable\n"
        + "group by hiredate";
    sql(sql)
        .withCatalogReaderFactory(catalogReaderFactory)
        .withRule(CoreRules.PROJECT_AGGREGATE_MERGE)
        .checkUnchanged();
  }

  @Test void testProjectAggregateMergeNoOpForNonSum() {
    final String sql = "select coalesce(m, 0)\n"
        + "from (\n"
        + "  select max(deptno) as m\n"
        + "  from sales.emp\n"
        + ")";
    sql(sql)
        .withRule(CoreRules.PROJECT_AGGREGATE_MERGE)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate.
   */
  @Test void testAggregateMerge1() {
    final String sql = "select deptno c, min(y), max(z) z,\n"
        + "sum(r), sum(m) n, sum(x) sal from (\n"
        + "   select deptno, ename, sum(sal) x, max(sal) z,\n"
        + "      min(sal) y, count(hiredate) m, count(mgr) r\n"
        + "   from sales.emp group by deptno, ename) t\n"
        + "group by deptno";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .check();
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate, top aggregate is not simple aggregate.
   */
  @Test void testAggregateMerge2() {
    final String sql = "select deptno, empno, sum(x), sum(y)\n"
        + "from (\n"
        + "  select ename, empno, deptno, sum(sal) x, count(mgr) y\n"
        + "    from sales.emp\n"
        + "  group by deptno, ename, empno) t\n"
        + "group by grouping sets(deptno, empno)";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .check();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since lower aggregate is not simple aggregate.
   */
  @Test void testAggregateMerge3() {
    final String sql = "select deptno, sum(x) from (\n"
        + " select ename, deptno, sum(sal) x from\n"
        + "   sales.emp group by cube(deptno, ename)) t\n"
        + "group by deptno";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since it contains distinct aggregate
   * function.
   */
  @Test void testAggregateMerge4() {
    final String sql = "select deptno, sum(x) from (\n"
        + "  select ename, deptno, count(distinct sal) x\n"
        + "    from sales.emp group by deptno, ename) t\n"
        + "group by deptno";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since AVG doesn't support splitting.
   */
  @Test void testAggregateMerge5() {
    final String sql = "select deptno, avg(x) from (\n"
        + "  select mgr, deptno, avg(sal) x from\n"
        + "    sales.emp group by deptno, mgr) t\n"
        + "group by deptno";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since top agg has no group key, and
   * lower agg function is COUNT.
   */
  @Test void testAggregateMerge6() {
    final String sql = "select sum(x) from (\n"
        + "select mgr, deptno, count(sal) x from\n"
        + "sales.emp group by deptno, mgr) t";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3957">[CALCITE-3957]
   * AggregateMergeRule should merge SUM0 into COUNT even if GROUP BY is
   * empty</a>. (It is not valid to merge a SUM onto a SUM0 if the top GROUP BY
   * is empty.) */
  @Test void testAggregateMergeSum0() {
    final String sql = "select coalesce(sum(count_comm), 0)\n"
        + "from (\n"
        + "  select deptno, count(comm) as count_comm\n"
        + "  from sales.emp\n"
        + "  group by deptno, mgr) t";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_AGGREGATE_MERGE,
            CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_MERGE)
        .check();
  }

  /**
   * Test case for AggregateMergeRule, should not merge 2 aggregates
   * into a single aggregate, since top agg contains empty grouping set,
   * and lower agg function is COUNT.
   */
  @Test void testAggregateMerge7() {
    final String sql = "select mgr, deptno, sum(x) from (\n"
        + "  select mgr, deptno, count(sal) x from\n"
        + "    sales.emp group by deptno, mgr) t\n"
        + "group by cube(mgr, deptno)";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .checkUnchanged();
  }

  /**
   * Test case for AggregateMergeRule, should merge 2 aggregates
   * into a single aggregate, since both top and bottom aggregates
   * contains empty grouping set and they are mergable.
   */
  @Test void testAggregateMerge8() {
    final String sql = "select sum(x) x, min(y) z from (\n"
        + "  select sum(sal) x, min(sal) y from sales.emp)";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6557">[CALCITE-6557]
   * AggregateMergeRule throws 'type mismatch' AssertionError</a>. The scenario
   * has the same aggregate function (SUM) at multiple levels; the lower level
   * is NOT NULL (because of GROUP BY) and the upper level is nullable. */
  @Test void testAggregateMerge9() {
    final String sql = "SELECT sum(deptno)\n"
        + "FROM (\n"
        + "    SELECT sum(deptno) AS deptno\n"
        + "    FROM dept\n"
        + "    GROUP BY name)";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_MERGE)
        .check();
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and all aggregate functions are splittable.
   */
  @Test void testAggregateRemove1() {
    final String sql = "select empno, sum(sal), min(sal), max(sal),\n"
        + " bit_and(distinct sal), bit_or(sal), count(distinct sal),\n"
        + " grouping(deptno)\n"
        + "from sales.emp\n"
        + "group by empno, deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_REMOVE,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and there are no aggregate functions.
   */
  @Test void testAggregateRemove2() {
    final String sql = "select distinct empno, deptno from sales.emp\n";
    sql(sql)
        .withRelBuilderConfig(b -> b.withAggregateUnique(true))
        .withRule(CoreRules.AGGREGATE_REMOVE,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  /**
   * Test case for AggregateRemoveRule, should remove aggregates since
   * empno is unique and all aggregate functions are splittable. Count
   * aggregate function should be transformed to CASE function call
   * because mgr is nullable.
   */
  @Test void testAggregateRemove3() {
    final String sql = "select empno, count(mgr) "
        + "from sales.emp group by empno, deptno\n";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_REMOVE,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove aggregate because avg is not splittable.
   */
  @Test void testAggregateRemove4() {
    final String sql = "select empno, max(sal), avg(sal) "
        + "from sales.emp group by empno, deptno\n";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_REMOVE,
            CoreRules.PROJECT_MERGE)
        .checkUnchanged();
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove non-simple aggregates.
   */
  @Test void testAggregateRemove5() {
    final String sql = "select empno, deptno, sum(sal) "
        + "from sales.emp group by cube(empno, deptno)\n";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_REMOVE,
            CoreRules.PROJECT_MERGE)
        .checkUnchanged();
  }

  /**
   * Negative test case for AggregateRemoveRule, should not
   * remove aggregate because deptno is not unique.
   */
  @Test void testAggregateRemove6() {
    final String sql = "select deptno, max(sal) "
        + "from sales.emp group by deptno\n";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_REMOVE,
            CoreRules.PROJECT_MERGE)
        .checkUnchanged();
  }

  /** Tests that top Aggregate is removed. Given "deptno=100", the
   * input of top Aggregate must be already distinct by "mgr". */
  @Test void testAggregateRemove7() {
    final String sql = ""
        + "select mgr, sum(sum_sal)\n"
        + "from\n"
        + "(select mgr, deptno, sum(sal) sum_sal\n"
        + " from sales.emp\n"
        + " group by mgr, deptno)\n"
        + "where deptno=100\n"
        + "group by mgr";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_REMOVE,
            CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2712">[CALCITE-2712]
   * Should remove the left join since the aggregate has no call and
   * only uses column in the left input of the bottom join as group key</a>. */
  @Test void testAggregateJoinRemove1() {
    final String sql = "select distinct e.deptno from sales.emp e\n"
        + "left outer join sales.dept d on e.deptno = d.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but has aggregate
   * call with distinct. */
  @Test void testAggregateJoinRemove2() {
    final String sql = "select e.deptno, count(distinct e.job)\n"
        + "from sales.emp e\n"
        + "left outer join sales.dept d on e.deptno = d.deptno\n"
        + "group by e.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but should not
   * remove the left join since the aggregate uses column in the right
   * input of the bottom join. */
  @Test void testAggregateJoinRemove3() {
    final String sql = "select e.deptno, count(distinct d.name)\n"
        + "from sales.emp e\n"
        + "left outer join sales.dept d on e.deptno = d.deptno\n"
        + "group by e.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()} but right join. */
  @Test void testAggregateJoinRemove4() {
    final String sql = "select distinct d.deptno\n"
        + "from sales.emp e\n"
        + "right outer join sales.dept d on e.deptno = d.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove2()} but right join. */
  @Test void testAggregateJoinRemove5() {
    final String sql = "select d.deptno, count(distinct d.name)\n"
        + "from sales.emp e\n"
        + "right outer join sales.dept d on e.deptno = d.deptno\n"
        + "group by d.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove3()} but right join. */
  @Test void testAggregateJoinRemove6() {
    final String sql = "select d.deptno, count(distinct e.job)\n"
        + "from sales.emp e\n"
        + "right outer join sales.dept d on e.deptno = d.deptno\n"
        + "group by d.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the bottom join since the aggregate has no aggregate
   * call. */
  @Test void testAggregateJoinRemove7() {
    final String sql = "SELECT distinct e.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_JOIN_REMOVE)
        .check();
  }


  /** Similar to {@link #testAggregateJoinRemove7()} but has aggregate
   * call. */
  @Test void testAggregateJoinRemove8() {
    final String sql = "SELECT e.deptno, COUNT(DISTINCT d2.name)\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno\n"
        + "GROUP BY e.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove7()} but use columns in
   * the right input of the top join. */
  @Test void testAggregateJoinRemove9() {
    final String sql = "SELECT distinct e.deptno, d2.name\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the bottom join since the aggregate uses column in the
   * right input of bottom join. */
  @Test void testAggregateJoinRemove10() {
    final String sql = "SELECT e.deptno, COUNT(DISTINCT d1.name, d2.name)\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d1 ON e.deptno = d1.deptno\n"
        + "LEFT JOIN sales.dept d2 ON e.deptno = d2.deptno\n"
        + "GROUP BY e.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove3()} but with agg call
   * referencing the last column of the left input. */
  @Test void testAggregateJoinRemove11() {
    final String sql = "select e.deptno, count(distinct e.slacker)\n"
        + "from sales.emp e\n"
        + "left outer join sales.dept d on e.deptno = d.deptno\n"
        + "group by e.deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_PROJECT_MERGE,
            CoreRules.AGGREGATE_JOIN_REMOVE)
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
    sql(sql).withRule(CoreRules.PROJECT_JOIN_JOIN_REMOVE)
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
    sql(sql).withRule(CoreRules.PROJECT_JOIN_JOIN_REMOVE)
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
    sql(sql).withRule(CoreRules.PROJECT_JOIN_JOIN_REMOVE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the left join since the join key of the right input is
   * unique. */
  @Test void testProjectJoinRemove4() {
    final String sql = "SELECT e.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d ON e.deptno = d.deptno";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the left join since the join key of the right input is
   * not unique. */
  @Test void testProjectJoinRemove5() {
    final String sql = "SELECT e1.deptno\n"
        + "FROM sales.emp e1\n"
        + "LEFT JOIN sales.emp e2 ON e1.deptno = e2.deptno";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_REMOVE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the left join since the project use columns in the right
   * input of the join. */
  @Test void testProjectJoinRemove6() {
    final String sql = "SELECT e.deptno, d.name\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d ON e.deptno = d.deptno";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_REMOVE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should remove the right join since the join key of the left input is
   * unique. */
  @Test void testProjectJoinRemove7() {
    final String sql = "SELECT e.deptno\n"
        + "FROM sales.dept d\n"
        + "RIGHT JOIN sales.emp e ON e.deptno = d.deptno";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_REMOVE)
        .check();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the right join since the join key of the left input is
   * not unique. */
  @Test void testProjectJoinRemove8() {
    final String sql = "SELECT e2.deptno\n"
        + "FROM sales.emp e1\n"
        + "RIGHT JOIN sales.emp e2 ON e1.deptno = e2.deptno";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_REMOVE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove1()};
   * Should not remove the right join since the project uses columns in the
   * left input of the join. */
  @Test void testProjectJoinRemove9() {
    final String sql = "SELECT e.deptno, d.name\n"
        + "FROM sales.dept d\n"
        + "RIGHT JOIN sales.emp e ON e.deptno = d.deptno";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_REMOVE)
        .checkUnchanged();
  }

  /** Similar to {@link #testAggregateJoinRemove4()};
   * The project references the last column of the left input.
   * The rule should be fired.*/
  @Test void testProjectJoinRemove10() {
    final String sql = "SELECT e.deptno, e.slacker\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d ON e.deptno = d.deptno";
    sql(sql).withRule(CoreRules.PROJECT_JOIN_REMOVE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7071">[CALCITE-7071]
   * Add test for replacing JOIN node with its child node
   * when JOIN condition is false</a>. */
  @Test void testLeftJoinRemoveConditionIsAlwaysFalse() {
    final String sql = "SELECT e.deptno\n"
        + "FROM sales.emp e\n"
        + "LEFT JOIN sales.dept d ON e.deptno = d.deptno and e.deptno between 3 and 1";
    sql(sql).withRule(CoreRules.JOIN_CONDITION_PUSH,
            PruneEmptyRules.JOIN_RIGHT_INSTANCE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7071">[CALCITE-7071]
   * Add test for replacing JOIN node with its child node
   * when JOIN condition is false</a>. */
  @Test void testRightJoinRemoveConditionIsAlwaysFalse() {
    final String sql = "SELECT e.deptno\n"
        + "FROM sales.dept d\n"
        + "RIGHT JOIN sales.emp e ON e.deptno = d.deptno and e.deptno between 3 and 1";
    sql(sql).withRule(CoreRules.JOIN_CONDITION_PUSH,
            PruneEmptyRules.JOIN_LEFT_INSTANCE)
        .check();
  }

  @Test void testSwapOuterJoin() {
    final HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(CoreRules.JOIN_COMMUTE_OUTER)
        .build();
    final String sql = "select 1 from sales.dept d left outer join sales.emp e\n"
        + " on d.deptno = e.deptno";
    sql(sql).withProgram(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7089">[CALCITE-7089]
   * Implement a rule for converting a RIGHT JOIN to a LEFT JOIN</a>. */
  @Test void testSwapRightToLeftOnly() {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.JOIN_COMMUTE_RIGHT_TO_LEFT)
        .build();
    final String sql = "select 1 from sales.dept d right outer join sales.emp e\n"
        + " on d.deptno = e.deptno";
    sql(sql).withProgram(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4042">[CALCITE-4042]
   * JoinCommuteRule must not match SEMI / ANTI join</a>. */
  @Test void testSwapSemiJoin() {
    checkSwapJoinShouldNotMatch(JoinRelType.SEMI);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4042">[CALCITE-4042]
   * JoinCommuteRule must not match SEMI / ANTI join</a>. */
  @Test void testSwapAntiJoin() {
    checkSwapJoinShouldNotMatch(JoinRelType.ANTI);
  }

  private void checkSwapJoinShouldNotMatch(JoinRelType type) {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT")
        .join(type,
            b.equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO")))
        .project(b.field("EMPNO"))
        .build();
    relFn(relFn).withRule(CoreRules.JOIN_COMMUTE_OUTER).checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4621">[CALCITE-4621]
   * SemiJoinRule throws AssertionError on ANTI join</a>. */
  @Test void testJoinToSemiJoinRuleOnAntiJoin() {
    checkSemiJoinRuleOnAntiJoin(CoreRules.JOIN_TO_SEMI_JOIN);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4621">[CALCITE-4621]
   * SemiJoinRule throws AssertionError on ANTI join</a>. */
  @Test void testProjectToSemiJoinRuleOnAntiJoin() {
    checkSemiJoinRuleOnAntiJoin(CoreRules.PROJECT_TO_SEMI_JOIN);
  }

  private void checkSemiJoinRuleOnAntiJoin(RelOptRule rule) {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("DEPT")
        .scan("EMP")
        .project(b.field("DEPTNO"))
        .distinct()
        .antiJoin(
            b.equals(
                b.field(2, 0, "DEPTNO"),
                b.field(2, 1, "DEPTNO")))
        .project(b.field("DNAME"))
        .build();
    relFn(relFn).withRule(rule).checkUnchanged();
  }

  @Test void testPushJoinCondDownToProject() {
    final String sql = "select d.deptno, e.deptno from sales.dept d, sales.emp e\n"
        + " where d.deptno + 10 = e.deptno * 2";
    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_PUSH_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4616">[CALCITE-4616]
   * AggregateUnionTransposeRule causes row type mismatch when some inputs have
   * unique grouping key</a>. */
  @Test void testAggregateUnionTransposeWithOneInputUnique() {
    final String sql = "select deptno, SUM(t) from (\n"
        + "select deptno, 1 as t from sales.emp e1\n"
        + "union all\n"
        + "select distinct deptno, 2 as t from sales.emp e2)\n"
        + "group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_UNION_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6642">[CALCITE-6642]
   * AggregateUnionTransposeRule throws an assertion error when creating children aggregates
   * when one input only has a non-nullable column</a>. */
  @Test void testAggregateUnionTransposeWithOneInputNonNullable() {
    final String sql = "select deptno, SUM(t) from (\n"
        + "select deptno, 1 as t from sales.emp e1\n"
        + "union all\n"
        + "select deptno, nullif(sal, 0) as t from sales.emp e2)\n"
        + "group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_UNION_TRANSPOSE)
        .check();
  }

  /** If all inputs to UNION are already unique, AggregateUnionTransposeRule is
   * a no-op. */
  @Test void testAggregateUnionTransposeWithAllInputsUnique() {
    final String sql = "select deptno, SUM(t) from (\n"
        + "select distinct deptno, 1 as t from sales.emp e1\n"
        + "union all\n"
        + "select distinct deptno, 2 as t from sales.emp e2)\n"
        + "group by deptno";
    sql(sql)
        .withRule(CoreRules.AGGREGATE_UNION_TRANSPOSE)
        .checkUnchanged();
  }

  @Test void testAggregateUnionTransposeWithTopLevelGroupSetRemapping() {
    final String sql = "select count(t1), t2 from (\n"
        + "select (case when deptno=0 then 1 else null end) as t1, 1 as t2 from sales.emp e1\n"
        + "union all\n"
        + "select (case when deptno=0 then 1 else null end) as t1, 2 as t2 from sales.emp e2)\n"
        + "group by t2";
    sql(sql)
        .withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_UNION_TRANSPOSE)
        .check();
  }

  @Test void testSortJoinTranspose1() {
    final String sql = "select * from sales.emp e left join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by sal limit 10";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .check();
  }

  @Test void testSortJoinTranspose2() {
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by name";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .check();
  }

  @Test void testSortJoinTranspose3() {
    // This one cannot be pushed down
    final String sql = "select * from sales.emp e left join (\n"
        + "  select * from sales.dept) d on e.deptno = d.deptno\n"
        + "order by sal, name limit 10";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-931">[CALCITE-931]
   * Wrong collation trait in SortJoinTransposeRule for right joins</a>. */
  @Test void testSortJoinTranspose4() {
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by name";
    sql(sql).withFactory(t ->
        t.withPlannerFactory(context ->
            // Create a customized test with RelCollation trait in the test
            // cluster.
            new MockRelOptPlanner(Contexts.empty()) {
              @Override public List<RelTraitDef> getRelTraitDefs() {
                return ImmutableList.of(RelCollationTraitDef.INSTANCE);
              }
              @Override public RelTraitSet emptyTraitSet() {
                return RelTraitSet.createEmpty().plus(
                    RelCollationTraitDef.INSTANCE.getDefault());
              }
            }))
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1498">[CALCITE-1498]
   * Avoid LIMIT with trivial ORDER BY being pushed through JOIN endlessly</a>. */
  @Test void testSortJoinTranspose5() {
    // SortJoinTransposeRule should not be fired again.
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "limit 10";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE,
            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1507">[CALCITE-1507]
   * OFFSET cannot be pushed through a JOIN if the non-preserved side of outer
   * join is not count-preserving</a>. */
  @Test void testSortJoinTranspose6() {
    // This one can be pushed down even if it has an OFFSET, since the dept
    // table is count-preserving against the join condition.
    final String sql = "select d.deptno, empno from sales.dept d\n"
        + "right join sales.emp e using (deptno) limit 10 offset 2";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1507">[CALCITE-1507]
   * OFFSET cannot be pushed through a JOIN if the non-preserved side of outer
   * join is not count-preserving</a>. */
  @Test void testSortJoinTranspose7() {
    // This one cannot be pushed down
    final String sql = "select d.deptno, empno from sales.dept d\n"
        + "left join sales.emp e using (deptno) order by d.deptno offset 1";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6983">[CALCITE-6983]
   * SortJoinTransposeRule should not push SORT past a JOIN when SORT's fetch is DynamicParam
   * </a>. */
  @Test void testSortJoinTranspose8() {
    final String sql = "select * from sales.emp e right join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "FETCH NEXT ? ROWS ONLY";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE,
            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6983">[CALCITE-6983]
   * SortJoinTransposeRule should not push SORT past a JOIN when SORT's fetch is DynamicParam
   * </a>. */
  @Test void testSortJoinTranspose9() {
    final String sql = "select * from sales.emp e left join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "OFFSET ? ROWS";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE,
            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_TRANSPOSE)
        .checkUnchanged();
  }

  @Test void testSortProjectTranspose1() {
    // This one can be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as integer) offset 1";
    sql(sql).withRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .check();
  }

  @Test void testSortProjectTranspose2() {
    // This one can be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as double) offset 1";
    sql(sql).withRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .check();
  }

  @Test void testSortProjectTranspose3() {
    // This one cannot be pushed down
    final String sql = "select d.deptno from sales.dept d\n"
        + "order by cast(d.deptno as varchar(10)) offset 1";
    sql(sql).withRule(CoreRules.SORT_JOIN_TRANSPOSE)
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
    sql(sql).withRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6586">[CALCITE-6586]
   * Some Rules not firing due to RelMdPredicates returning null in VolcanoPlanner</a>. */
  @Test void testAggregateConstantKeyRuleInVolcano() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno, sal";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        })
        .check();
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where reduction is not
   * possible because "deptno" is the only key. */
  @Test void testAggregateConstantKeyRule2() {
    final String sql = "select count(*) as c\n"
        + "from sales.emp\n"
        + "where deptno = 10\n"
        + "group by deptno";
    sql(sql).withRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS)
        .checkUnchanged();
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where both keys are
   * constants but only one can be removed. */
  @Test void testAggregateConstantKeyRule3() {
    final String sql = "select job\n"
        + "from sales.emp\n"
        + "where mgr is null and job = 'Clerk'\n"
        + "group by mgr, job\n"
        + "having count(*) > 3";
    sql(sql).withRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS)
        .check();
  }

  /** Tests {@link AggregateProjectPullUpConstantsRule} where
   * there are group keys of type
   * {@link org.apache.calcite.sql.fun.SqlAbstractTimeFunction}
   * that can not be removed. */
  @Test void testAggregateDynamicFunction() {
    final String sql = "select hiredate\n"
        + "from sales.emp\n"
        + "where mgr is null and hiredate = current_timestamp\n"
        + "group by mgr, hiredate\n"
        + "having count(*) > 3";
    sql(sql).withRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS)
        .check();
  }

  @Test void testReduceExpressionsNot() {
    final String sql = "select * from (values (false),(true)) as q (col1) where not(col1)";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  /** Tests expanding a sub-query, specifically an uncorrelated scalar
   * sub-query in a project (SELECT clause). */
  @Test void testExpandProjectScalar() {
    final String sql = "select empno,\n"
        + "  (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testSelectNotInCorrelated() {
    final String sql = "select sal,\n"
        + " empno NOT IN (\n"
        + " select deptno from dept\n"
        + "   where emp.job=dept.name)\n"
        + " from emp";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1493">[CALCITE-1493]
   * Wrong plan for NOT IN correlated queries</a>. */
  @Test void testWhereNotInCorrelated() {
    final String sql = "select sal from emp\n"
        + "where empno NOT IN (\n"
        + "  select deptno from dept\n"
        + "  where emp.job = dept.name)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testWhereNotInCorrelated2() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno NOT IN\n"
        + "   (select empno from (select ename, empno, sal as r from emp) e2\n"
        + "    where r > 2 and e1.ename= e2.ename)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testAll() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno > ALL (select deptno from dept)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testSome() {
    final String sql = "select * from emp e1\n"
        + "  where e1.empno > SOME (select deptno from dept)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for testing type created by SubQueryRemoveRule: an
   * ANY sub-query is non-nullable therefore plan should have cast. */
  @Test void testAnyInProjectNonNullable() {
    final String sql = "select name, deptno > ANY (\n"
        + "  select deptno from emp)\n"
        + "from dept";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for testing type created by SubQueryRemoveRule; an
   * ANY sub-query is nullable therefore plan should not have cast. */
  @Test void testAnyInProjectNullable() {
    final String sql = "select deptno, name = ANY (\n"
        + "  select mgr from emp)\n"
        + "from dept";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testSelectAnyCorrelated() {
    final String sql = "select empno > ANY (\n"
        + "  select deptno from dept where emp.job = dept.name)\n"
        + "from emp\n";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testWhereAnyCorrelatedInSelect() {
    final String sql = "select * from emp where empno > ANY (\n"
        + "  select deptno from dept where emp.job = dept.name)\n";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testSomeWithEquality() {
    final String sql = "select * from emp e1\n"
        + "  where e1.deptno = SOME (select deptno from dept)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testSomeWithEquality2() {
    final String sql = "select * from emp e1\n"
        + "  where e1.ename= SOME (select name from dept)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testSomeWithNotEquality() {
    final String sql = "select * from emp e1\n"
        + "  where e1.deptno <> SOME (select deptno from dept)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5655">[CALCITE-5655]
   * Wrong field reference lookup due to same intermediate table alias
   * of multiple sub-queries in subquery remove phase</a>. */
  @Test public void testSomeWithTwoCorrelatedSubQueries() {
    final String sql = "select empno from sales.empnullables as e\n"
        + "where deptno > some(\n"
        + "  select deptno from sales.deptnullables where e.ename = name and deptno > 10)\n"
        + "or deptno < some(\n"
        + "  select deptno from sales.deptnullables where e.ename = name and deptno < 20)";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .withTrim(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5655">[CALCITE-5655]
   * Wrong field reference lookup due to same intermediate table alias
   * of multiple sub-queries in subquery remove phase</a>. */
  @Test public void testSomeWithTwoSubQueries() {
    final String sql = "select empno from sales.empnullables\n"
        + "where deptno > some(\n"
        + "  select deptno from sales.deptnullables where name = 'dept1')\n"
        + "or deptno < some(\n"
        + "  select deptno from sales.deptnullables where name = 'dept2')";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .withTrim(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1546">[CALCITE-1546]
   * Sub-queries connected by OR</a>. */
  @Test void testWhereOrSubQuery() {
    final String sql = "select * from emp\n"
        + "where sal = 4\n"
        + "or empno NOT IN (select deptno from dept)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testExpandProjectIn() {
    final String sql = "select empno,\n"
        + "  deptno in (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .check();
  }

  @Test void testExpandProjectInNullable() {
    final String sql = "with e2 as (\n"
        + "  select empno, case when true then deptno else null end as deptno\n"
        + "  from sales.emp)\n"
        + "select empno,\n"
        + "  deptno in (select deptno from e2 where empno < 20) as d\n"
        + "from e2";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .check();
  }

  @Test void testExpandProjectInWithTwoCorrelatedSubQueries() {
    final String sql = "select empno, deptno in (\n"
        + "  select deptno from sales.deptnullables where e.ename = name and deptno > 10)\n"
        + "or deptno in (\n"
        + "  select deptno from sales.deptnullables where e.ename = name and deptno < 20)\n"
        + "from sales.empnullables as e";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .withTrim(true)
        .check();
  }

  @Test void testExpandProjectInWithTwoSubQueries() {
    final String sql = "select empno, deptno in (\n"
        + "  select deptno from sales.deptnullables where name = 'dept1')\n"
        + "or deptno in (\n"
        + "  select deptno from sales.deptnullables where name = 'dept2')\n"
        + "from sales.empnullables";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .withTrim(true)
        .check();
  }

  @Test void testExpandProjectInComposite() {
    final String sql = "select empno, (empno, deptno) in (\n"
        + "    select empno, deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .check();
  }

  @Test void testExpandProjectExists() {
    final String sql = "select empno,\n"
        + "  exists (select deptno from sales.emp where empno < 20) as d\n"
        + "from sales.emp";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .check();
  }

  @Test void testExpandFilterScalar() {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where (select deptno from sales.emp where empno < 20)\n"
        + " < (select deptno from sales.emp where empno > 100)\n"
        + "or emp.sal < 100";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testExpandFilterIn() {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where deptno in (select deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testExpandFilterInComposite() {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where (empno, deptno) in (\n"
        + "  select empno, deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    sql(sql).withSubQueryRules().check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5655">[CALCITE-5655]
   * Wrong field reference lookup due to same intermediate table alias
   * of multiple sub-queries in subquery remove phase</a>. */
  @Test void testExpandFilterInCorrelatedWithTwoSubQueries() {
    final String sql = "select empno from sales.empnullables as e\n"
        + "where deptno in (\n"
        + "  select deptno from sales.deptnullables where e.ename = name and deptno > 10)\n"
        + "or deptno in (\n"
        + "  select deptno from sales.deptnullables where e.ename = name and deptno < 20)";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .withTrim(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5655">[CALCITE-5655]
   * Wrong field reference lookup due to same intermediate table alias
   * of multiple sub-queries in subquery remove phase</a>. */
  @Test void testExpandFilterInWithTwoSubQueries() {
    final String sql = "select empno from sales.empnullables\n"
        + "where deptno in (\n"
        + "  select deptno from sales.deptnullables where name = 'dept1')\n"
        + "or deptno in (\n"
        + "  select deptno from sales.deptnullables where name = 'dept2')";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .withTrim(true)
        .check();
  }

  /** An IN filter that requires full 3-value logic (true, false, unknown). */
  @Test void testExpandFilterIn3Value() {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where empno\n"
        + " < case deptno in (select case when true then deptno else null end\n"
        + "                   from sales.emp where empno < 20)\n"
        + "   when true then 10\n"
        + "   when false then 20\n"
        + "   else 30\n"
        + "   end";
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5680">[CALCITE-5680]
   * Wrong field reference lookup due to same intermediate table alias
   * of multiple sub-queries with only literal operands in subquery remove phase</a>. */
  @Test void testExpandFilterConstantInCorrelatedWithTwoSubQueries() {
    final String sql = "select empno from sales.empnullables as e\n"
        + "where 1 in (\n"
        + "  select deptno from sales.deptnullables where e.ename = name and deptno > 10)\n"
        + "or 2 in (\n"
        + "  select deptno from sales.deptnullables where e.ename = name and deptno < 20)";
    // We disable expression simplification and enable trim to make plan
    // more straightforward and easier to identify whether the plan is correct
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .withTrim(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5680">[CALCITE-5680]
   * Wrong field reference lookup due to same intermediate table alias
   * of multiple sub-queries with only literal operands in subquery remove phase</a>. */
  @Test void testExpandFilterConstantInWithTwoSubQueries() {
    final String sql = "select empno from sales.empnullables\n"
        + "where 1 in (\n"
        + "  select deptno from sales.deptnullables where name = 'dept1')\n"
        + "or 2 in (\n"
        + "  select deptno from sales.deptnullables where name = 'dept2')";
    // We disable expression simplification and enable trim to make plan
    // more straightforward and easier to identify whether the plan is correct
    sql(sql)
        .withSubQueryRules()
        .withRelBuilderSimplify(false)
        .withTrim(true)
        .check();
  }

  /** An EXISTS filter that can be converted into true/false. */
  @Test void testExpandFilterExists() {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)\n"
        + "or emp.sal < 100";
    sql(sql).withSubQueryRules().check();
  }

  /** An EXISTS filter that can be converted into a semi-join. */
  @Test void testExpandFilterExistsSimple() {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)";
    sql(sql).withSubQueryRules().check();
  }

  /** An EXISTS filter that can be converted into a semi-join. */
  @Test void testExpandFilterExistsSimpleAnd() {
    final String sql = "select empno\n"
        + "from sales.emp\n"
        + "where exists (select deptno from sales.emp where empno < 20)\n"
        + "and emp.sal < 100";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testExpandJoinScalar() {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on (select deptno from sales.emp where empno < 20)\n"
        + " < (select deptno from sales.emp where empno > 100)";
    sql(sql).withSubQueryRules().check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3121">[CALCITE-3121]
   * VolcanoPlanner hangs due to sub-query with dynamic star</a>. */
  @Test void testSubQueryWithDynamicStarHang() {
    String sql = "select n.n_regionkey\n"
        + "from (select *\n"
        + "  from (select *\n"
        + "    from sales.customer) t) n\n"
        + "where n.n_nationkey > 1";

    PushProjector.ExprCondition exprCondition = expr -> {
      if (expr instanceof RexCall) {
        RexCall call = (RexCall) expr;
        return "item".equals(call.getOperator().getName().toLowerCase(Locale.ROOT));
      }
      return false;
    };
    RuleSet ruleSet =
        RuleSets.ofList(CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_MERGE,
            CoreRules.PROJECT_MERGE,
            ProjectFilterTransposeRule.Config.DEFAULT
                .withOperandFor(Project.class, Filter.class)
                .withPreserveExprCondition(exprCondition)
                .toRule(),
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    Program program = Programs.of(ruleSet);

    sql(sql)
        .withVolcanoPlanner(false)
        .withDynamicTable()
        .withDecorrelate(true)
        .withAfter((fixture, r) -> {
          RelTraitSet toTraits =
              r.getCluster().traitSet()
                  .replace(0, EnumerableConvention.INSTANCE);
          return program.run(fixture.planner, r, toTraits,
              ImmutableList.of(), ImmutableList.of());
        })
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3188">[CALCITE-3188]
   * IndexOutOfBoundsException in ProjectFilterTransposeRule when executing SELECT COUNT(*)</a>. */
  @Test void testProjectFilterTransposeRuleOnEmptyRowType() {
    // build a rel equivalent to sql:
    // select `empty` from emp
    // where emp.deptno = 20
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(b
            .equals(
                b.field(1, 0, "DEPTNO"),
                b.literal(20)))
        .project(ImmutableList.of())
        .build();
    relFn(relFn).withRule(CoreRules.PROJECT_FILTER_TRANSPOSE).check();
  }

  @Test void testFlattenUncorrelatedCallBelowEquals() {
    final String sql = "select * from emp e1 where exists ("
        + "select * from emp e2 where e1.deptno = (e2.deptno+30))";
    sql(sql).withDecorrelate(false)
        .withExpand(true)
        .withRule(FilterFlattenCorrelatedConditionRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testCallOverCorrelationVariableIsNotFlattened() {
    final String sql = "select * from emp e1 where exists ("
        + "select * from emp e2 where (e1.deptno+30) = e2.deptno)";
    sql(sql).withDecorrelate(false)
        .withExpand(true)
        .withRule(FilterFlattenCorrelatedConditionRule.Config.DEFAULT.toRule())
        .checkUnchanged();
  }

  @Test void testFlattenUncorrelatedTwoLevelCallBelowEqualsSucceeds() {
    final String sql = "select * from emp e1 where exists ("
        + "select * from emp e2 where e1.deptno = (2 * e2.deptno+30))";
    sql(sql).withDecorrelate(false)
        .withExpand(true)
        .withRule(FilterFlattenCorrelatedConditionRule.Config.DEFAULT.toRule())
        .check();
  }

  @Test void testUncorrelatedCallBelowNonComparisonOpIsNotFlattened() {
    final String sql = "select * from emp e1 where exists ("
        + "select * from emp e2 where (e1.deptno + (e2.deptno+30)) > 0)";
    sql(sql).withDecorrelate(false)
        .withExpand(true)
        .withRule(FilterFlattenCorrelatedConditionRule.Config.DEFAULT.toRule())
        .checkUnchanged();
  }

  @Test void testUncorrelatedCallInConjunctionIsFlattenedOnlyIfSiblingOfCorrelation() {
    final String sql = "select * from emp e1 where exists ("
        + "select * from emp e2 where (e2.empno+50) < 20 and e1.deptno >= (30+e2.deptno))";
    sql(sql).withDecorrelate(false)
        .withExpand(true)
        .withRule(FilterFlattenCorrelatedConditionRule.Config.DEFAULT.toRule())
        .check();
  }

  @Disabled("[CALCITE-1045]")
  @Test void testExpandJoinIn() {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on emp.deptno in (select deptno from sales.emp where empno < 20)";
    sql(sql).withSubQueryRules().check();
  }

  @Disabled("[CALCITE-1045]")
  @Test void testExpandJoinInComposite() {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on (emp.empno, dept.deptno) in (\n"
        + "  select empno, deptno from sales.emp where empno < 20)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testExpandJoinExists() {
    final String sql = "select empno\n"
        + "from sales.emp left join sales.dept\n"
        + "on exists (select deptno from sales.emp where empno < 20)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testDecorrelateExists() {
    final String sql = "select * from sales.emp\n"
        + "where EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1511">[CALCITE-1511]
   * AssertionError while decorrelating query with two EXISTS
   * sub-queries</a>. */
  @Test void testDecorrelateTwoExists() {
    final String sql = "select * from sales.emp\n"
        + "where EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)\n"
        + "AND NOT EXISTS (\n"
        + "  select * from emp ee where ee.job = emp.job AND ee.sal=34)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2028">[CALCITE-2028]
   * Un-correlated IN sub-query should be converted into a Join,
   * rather than a Correlate without correlation variables </a>. */
  @Test void testDecorrelateUncorrelatedInAndCorrelatedExists() {
    final String sql = "select * from sales.emp\n"
        + "WHERE job in (\n"
        + "  select job from emp ee where ee.sal=34)"
        + "AND EXISTS (\n"
        + "  select * from emp e where emp.deptno = e.deptno)\n";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1537">[CALCITE-1537]
   * Unnecessary project expression in multi-sub-query plan</a>. */
  @Test void testDecorrelateTwoIn() {
    final String sql = "select sal\n"
        + "from sales.emp\n"
        + "where empno IN (\n"
        + "  select deptno from dept where emp.job = dept.name)\n"
        + "AND empno IN (\n"
        + "  select empno from emp e where emp.ename = e.ename)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1045">[CALCITE-1045]
   * Decorrelate sub-queries in Project and Join</a>, with the added
   * complication that there are two sub-queries. */
  @Disabled("[CALCITE-1045]")
  @Test void testDecorrelateTwoScalar() {
    final String sql = "select deptno,\n"
        + "  (select min(1) from emp where empno > d.deptno) as i0,\n"
        + "  (select min(0) from emp\n"
        + "    where deptno = d.deptno and ename = 'SMITH') as i1\n"
        + "from dept as d";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testWhereInJoinCorrelated() {
    final String sql = "select empno from emp as e\n"
        + "join dept as d using (deptno)\n"
        + "where e.sal in (\n"
        + "  select e2.sal from emp as e2 where e2.deptno > e.deptno)";
    sql(sql).withSubQueryRules().check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1494">[CALCITE-1494]
   * Inefficient plan for correlated sub-queries</a>. In "planAfter", there
   * must be only one scan each of emp and dept. We don't need a separate
   * value-generator for emp.job. */
  @Test void testWhereInCorrelated() {
    final String sql = "select sal from emp where empno IN (\n"
        + "  select deptno from dept where emp.job = dept.name)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testWhereExpressionInCorrelated() {
    final String sql = "select ename from (\n"
        + "  select ename, deptno, sal + 1 as salPlus from emp) as e\n"
        + "where deptno in (\n"
        + "  select deptno from emp where sal + 1 = e.salPlus)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testWhereExpressionInCorrelated2() {
    final String sql = "select name from (\n"
        + "  select name, deptno, deptno - 10 as deptnoMinus from dept) as d\n"
        + "where deptno in (\n"
        + "  select deptno from emp where sal + 1 = d.deptnoMinus)";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  @Test void testExpandWhereComparisonCorrelated() {
    final String sql = "select empno\n"
        + "from sales.emp as e\n"
        + "where sal = (\n"
        + "  select max(sal) from sales.emp e2 where e2.empno = e.empno)";
    sql(sql).withSubQueryRules().check();
  }

  @Test void testCustomColumnResolvingInNonCorrelatedSubQuery() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 in (\n"
        + "  select f1.c0 from struct.t t2)";
    sql(sql).withSubQueryRules().withTrim(true).check();
  }

  @Test void testCustomColumnResolvingInCorrelatedSubQuery() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 = (\n"
        + "  select max(f1.c0) from struct.t t2 where t1.k0 = t2.k0)";
    sql(sql).withSubQueryRules().withTrim(true).check();
  }

  @Test void testCustomColumnResolvingInCorrelatedSubQuery2() {
    final String sql = "select *\n"
        + "from struct.t t1\n"
        + "where c0 in (\n"
        + "  select f1.c0 from struct.t t2 where t1.c2 = t2.c2)";
    sql(sql).withSubQueryRules().withTrim(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2744">[CALCITE-2744]
   * RelDecorrelator use wrong output map for LogicalAggregate decorrelate</a>. */
  @Test void testDecorrelateAggWithConstantGroupKey() {
    final String sql = "SELECT *\n"
        + "FROM emp A\n"
        + "where sal in (SELECT max(sal)\n"
        + "  FROM emp B\n"
        + "  where A.mgr = B.empno\n"
        + "  group by deptno, 'abc')";
    sql(sql)
        .withExpand(true)
        .withLateDecorrelate(true)
        .withTrim(true)
        .withRule() // empty program
        .check();
  }

  /** Test case for CALCITE-2744 for aggregate decorrelate with multi-param agg call
   * but without group key. */
  @Test void testDecorrelateAggWithMultiParamsAggCall() {
    final String sql = "SELECT * FROM (SELECT MYAGG(sal, 1) AS c FROM emp) as m,\n"
        + " LATERAL TABLE(ramp(m.c)) AS T(s)";
    sql(sql)
        .withLateDecorrelate(true)
        .withTrim(true)
        .withRule() // empty program
        .checkUnchanged();
  }

  /** Same as {@link #testDecorrelateAggWithMultiParamsAggCall}
   * but with a constant group key. */
  @Test void testDecorrelateAggWithMultiParamsAggCall2() {
    final String sql = "SELECT * FROM "
        + "(SELECT MYAGG(sal, 1) AS c FROM emp group by empno, 'abc') as m,\n"
        + " LATERAL TABLE(ramp(m.c)) AS T(s)";
    sql(sql)
        .withLateDecorrelate(true)
        .withTrim(true)
        .withRule() // empty program
        .checkUnchanged();
  }

  /** Test case for CALCITE-5683 for two level nested decorrelate with standard program
   * failing during the decorrelation phase. The correlation variable is used at two levels
   * deep. */
  @Test void testTwoLevelDecorrelate() {
    final String sql = "SELECT d1.name, d1.deptno + (\n"
        + "SELECT e1.empno\n"
        + "FROM emp e1\n"
        + "WHERE d1.deptno = e1.deptno and\n"
        + "    e1.sal = (SELECT max(sal)\n"
        + "              FROM emp e2\n"
        + "              WHERE e1.sal = e2.sal and\n"
        + "                  e1.deptno = e2.deptno and\n"
        + "                  d1.deptno < e2.deptno))\n"
        + "FROM dept d1";

    sql(sql)
        .withSubQueryRules()
        .withLateDecorrelate(true)
        .withTrim(true)
        .check();
  }

  /**
   * Test case that SubQueryRemoveRule works with correlated Filter without varibles.
   */
  @Test void testCorrelatedFilterWithoutVariable() {
    // select *
    // from dept
    // where exists (select deptno
    //               from emp
    //               where dept.deptno = emp.deptno
    //                 and emp.sal > 100)
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("DEPT")
        .variable(v::set)
        .filter(
            b.exists(b1 -> b1
            .scan("EMP")
            .filter(
                b1.and(
                b1.equals(b1.field(v.get(), "DEPTNO"), b1.field("DEPTNO")),
                b1.greaterThan(b1.field("SAL"), b1.literal(100))))
            .project(b1.field("DEPTNO"))
            .build()))
        .build();
    relFn(relFn)
        .withSubQueryRules()
        .check();
  }

  /** Test case for CALCITE-5683 for two level nested decorrelate with standard program
   * failing during the decorrelation phase. The correlation variable is used at the second
   * level and is not used in the first level */
  @Test void testCorrelatedVariableAtSecondLevel() {
    final String sql = "SELECT d1.name, d1.deptno +(\n"
        + "SELECT e1.empno\n"
        + "FROM emp e1\n"
        + "WHERE e1.sal = (SELECT max(sal)\n"
        + "                FROM emp e2\n"
        + "                WHERE e1.sal = e2.sal and\n"
        + "                    e1.deptno = e2.deptno and\n"
        + "                    d1.deptno < e2.deptno))\n"
        + "FROM dept d1";
    sql(sql)
        .withSubQueryRules()
        .withLateDecorrelate(true)
        .withTrim(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6652">[CALCITE-6652]
   * RelDecorrelator can't decorrelate query with limit 1</a>.
   */
  @Test void testDecorrelateProjectWithFetchOne() {
    final String query = "SELECT name, "
        + "(SELECT sal FROM emp where dept.deptno = emp.deptno order by sal limit 1) "
        + "FROM dept";
    sql(query).withRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .withLateDecorrelate(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6652">[CALCITE-6652]
   * RelDecorrelator can't decorrelate query with limit 1</a>.
   */
  @Test void testDecorrelateProjectWithFetchOneDesc() {
    final String query = "SELECT name, "
        + "(SELECT emp.sal FROM emp WHERE dept.deptno = emp.deptno "
        + "ORDER BY emp.sal desc nulls last LIMIT 1) "
        + "FROM dept";
    sql(query).withRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .withLateDecorrelate(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6652">[CALCITE-6652]
   * RelDecorrelator can't decorrelate query with limit 1</a>.
   */
  @Test void testDecorrelateFilterWithFetchOne() {
    final String query = "SELECT name FROM dept "
        + "WHERE 10 > (SELECT emp.sal FROM emp where dept.deptno = emp.deptno "
        + "ORDER BY emp.sal limit 1)";
    sql(query).withRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withLateDecorrelate(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6652">[CALCITE-6652]
   * RelDecorrelator can't decorrelate query with limit 1</a>.
   */
  @Test void testDecorrelateFilterWithFetchOneDesc() {
    final String query = "SELECT name FROM dept "
        + "WHERE 10 > (SELECT emp.sal FROM emp where dept.deptno = emp.deptno "
        + "ORDER BY emp.sal desc nulls last limit 1)";
    sql(query).withRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withLateDecorrelate(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6652">[CALCITE-6652]
   * RelDecorrelator can't decorrelate query with limit 1</a>.
   */
  @Test void testDecorrelateFilterWithFetchOneDesc1() {
    final String query = "SELECT name FROM dept "
        + "WHERE 10 > (SELECT emp.sal FROM emp where dept.deptno = emp.deptno "
        + "ORDER BY emp.sal desc limit 1)";
    sql(query).withRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withLateDecorrelate(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6652">[CALCITE-6652]
   * RelDecorrelator can't decorrelate query with limit 1</a>.
   */
  @Test void testDecorrelateProjectWithMultiKeyAndFetchOne() {
    final String query = "SELECT name, "
        + "(SELECT sal FROM emp where dept.deptno = emp.deptno "
        + "order by year(hiredate), emp.sal limit 1) FROM dept";
    sql(query).withRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .withLateDecorrelate(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6652">[CALCITE-6652]
   * RelDecorrelator can't decorrelate query with limit 1</a>.
   */
  @Test void testDecorrelateProjectWithMultiKeyAndFetchOne1() {
    final String query = "SELECT name, "
        + "(SELECT sal FROM emp where dept.deptno = emp.deptno and dept.name = emp.ename "
        + "order by year(hiredate), emp.sal limit 1) FROM dept";
    sql(query).withRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
        .withLateDecorrelate(true)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6652">[CALCITE-6652]
   * RelDecorrelator can't decorrelate query with limit 1</a>.
   */
  @Test void testDecorrelateFilterWithMultiKeyAndFetchOne() {
    final String query = "SELECT name FROM dept "
        + "WHERE 10 > (SELECT emp.sal FROM emp where dept.deptno = emp.deptno "
        + "order by year(hiredate), emp.sal desc limit 1)";
    sql(query).withRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
        .withLateDecorrelate(true)
        .check();
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
    sql(sql).withRule(DateRangeRules.FILTER_INSTANCE)
        .withContext(c -> Contexts.of(CalciteConnectionConfig.DEFAULT, c))
        .check();
  }

  @Test void testExtractYearMonthToRange() {
    final String sql = "select *\n"
        + "from sales.emp_b as e\n"
        + "where extract(year from birthdate) = 2014"
        + "and extract(month from birthdate) = 4";
    sql(sql).withRule(DateRangeRules.FILTER_INSTANCE)
        .withContext(c -> Contexts.of(CalciteConnectionConfig.DEFAULT, c))
        .check();
  }

  @Test void testFilterRemoveIsNotDistinctFromRule() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.call(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
            b.field("DEPTNO"), b.literal(20)))
        .build();
    relFn(relFn).withRule(CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6927">[CALCITE-6927]
   * Add rule for join condition remove IS NOT DISTINCT FROM</a>. */
  @Test void testJoinConditinExpandIsNotDistinctFromRule() {
    final String sql = ""
        + "select t1.ENAME from empnullables as t1\n"
        + "join\n"
        + "empnullables as t2\n"
        + "on t1.ENAME is not distinct from t2.ENAME";
    sql(sql)
        .withRule(CoreRules.JOIN_CONDITION_EXPAND_IS_NOT_DISTINCT_FROM)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6927">[CALCITE-6927]
   * Add rule for join condition remove IS NOT DISTINCT FROM</a>. */
  @Test void testJoinConditinExpandIsNotDistinctFromRuleArray() {
    final String sql = ""
        + "select t1.ADMINS from DEPT_NESTED_EXPANDED as t1\n"
        + "join\n"
        + "DEPT_NESTED_EXPANDED as t2\n"
        + "on t1.ADMINS is not distinct from t2.ADMINS";
    sql(sql)
        .withRule(CoreRules.JOIN_CONDITION_EXPAND_IS_NOT_DISTINCT_FROM)
        .check();
  }

  /** Creates an environment for testing spatial queries. */
  private RelOptFixture spatial(String sql) {
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .addRuleInstance(SpatialRules.INSTANCE)
        .build();
    return sql(sql)
        .withCatalogReaderFactory(MockCatalogReaderExtended::create)
        .withConformance(SqlConformanceEnum.LENIENT)
        .withProgram(program);
  }

  /** Tests that a call to {@code ST_DWithin}
   * is rewritten with an additional range predicate. */
  @Test void testSpatialDWithinToHilbert() {
    final String sql = "select *\n"
        + "from GEO.Restaurants as r\n"
        + "where ST_DWithin(ST_Point(10.0, 20.0),\n"
        + "                 ST_Point(r.longitude, r.latitude), 10)";
    spatial(sql).check();
  }

  /** Tests that a call to {@code ST_DWithin}
   * is rewritten with an additional range predicate. */
  @Test void testSpatialDWithinToHilbertZero() {
    final String sql = "select *\n"
        + "from GEO.Restaurants as r\n"
        + "where ST_DWithin(ST_Point(10.0, 20.0),\n"
        + "                 ST_Point(r.longitude, r.latitude), 0)";
    spatial(sql).check();
  }

  @Test void testSpatialDWithinToHilbertNegative() {
    final String sql = "select *\n"
        + "from GEO.Restaurants as r\n"
        + "where ST_DWithin(ST_Point(10.0, 20.0),\n"
        + "                 ST_Point(r.longitude, r.latitude), -2)";
    spatial(sql).check();
  }

  /** As {@link #testSpatialDWithinToHilbert()} but arguments reversed. */
  @Test void testSpatialDWithinReversed() {
    final String sql = "select *\n"
        + "from GEO.Restaurants as r\n"
        + "where ST_DWithin(ST_Point(r.longitude, r.latitude),\n"
        + "                 ST_Point(10.0, 20.0), 6)";
    spatial(sql).check();
  }

  /** Points within a given distance of a line. */
  @Test void testSpatialDWithinLine() {
    final String sql = "select *\n"
        + "from GEO.Restaurants as r\n"
        + "where ST_DWithin(\n"
        + "  ST_MakeLine(ST_Point(8.0, 20.0), ST_Point(12.0, 20.0)),\n"
        + "  ST_Point(r.longitude, r.latitude), 4)";
    spatial(sql).check();
  }

  /** Points near a constant point, using ST_Contains and ST_Buffer. */
  @Test void testSpatialContainsPoint() {
    final String sql = "select *\n"
        + "from GEO.Restaurants as r\n"
        + "where ST_Contains(\n"
        + "  ST_Buffer(ST_Point(10.0, 20.0), 6),\n"
        + "  ST_Point(r.longitude, r.latitude))";
    spatial(sql).check();
  }

  /** Constant reduction on geo-spatial expression. */
  @Test void testSpatialReduce() {
    final String sql = "select\n"
        + "  ST_Buffer(ST_Point(0.0, 0.0), 1, 4) as b\n"
        + "from GEO.Restaurants as r";
    spatial(sql)
        .withRelBuilderSimplify(false)
        .check();
  }

  @Test void testOversimplifiedCaseStatement() {
    String sql = "select * from emp "
        + "where MGR > 0 and "
        + "case when MGR > 0 then deptno / MGR else null end > 1";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
  * <a href="https://issues.apache.org/jira/browse/CALCITE-2726">[CALCITE-2726]
  * ReduceExpressionRule may oversimplify filter conditions containing nulls</a>.
  */
  @Test void testNoOversimplificationBelowIsNull() {
    String sql = "select *\n"
        + "from emp\n"
        + "where ( (empno=1 and mgr=1) or (empno=null and mgr=1) ) is null";
    sql(sql).withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6930">[CALCITE-6930]
   * Implementing JoinConditionOrExpansionRule</a>. */
  @Test void testJoinConditionOrExpansionRule() {
    String sql = "select *\n"
        + "from EMP as p1\n"
        + "inner join EMP as p2 on p1.empno = p2.empno or p1.mgr = p2.mgr";
    sql(sql).withRule(CoreRules.JOIN_EXPAND_OR_TO_UNION_RULE)
        .check();
  }

  @Test void testJoinConditionOrExpansionRule2() {
    String sql = "select * from empnullables as t1 inner join empnullables as t2\n"
        + "on (t1.empno = t2.empno\n"
        + "and t1.job = 'Job1'\n"
        + "and t1.ename in ('a', 'bb', 'cc')\n"
        + "and t1.sal > 120 and t1.sal < 3000\n"
        + "and t1.mgr = t1.comm)\n"
        + "or\n"
        + "(t1.deptno = t2.deptno\n"
        + "and t2.job = 'Job2'\n"
        + "and t2.ename in ('a', 'bb', 'cc')\n"
        + "and t2.sal > 110 and t2.sal < 3000\n"
        + "and t1.mgr + 10 < ln(15))\n"
        + "or\n"
        + "(t1.ename = 'Jensen'\n"
        + "and t2.comm > 10)\n"
        + "or\n"
        + "t1.mgr between 10.0 and 20\n";

    sql(sql).withRule(CoreRules.JOIN_EXPAND_OR_TO_UNION_RULE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6930">[CALCITE-6930]
   * Implementing JoinConditionOrExpansionRule</a>. */
  @Test void testJoinConditionOrExpansionRuleMultiOr() {
    String sql = "select *\n"
        + "from EMP as p1\n"
        + "inner join EMP as p2 on  p1.mgr < p2.mgr or\n"
        + "p1.empno = p2.empno or p1.sal < 0 or ln(p1.sal) < 10";
    sql(sql).withRule(CoreRules.JOIN_EXPAND_OR_TO_UNION_RULE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6930">[CALCITE-6930]
   * Implementing JoinConditionOrExpansionRule</a>. */
  @Test void testJoinConditionOrExpansionRuleRight() {
    String sql = "select *\n"
        + "from EMP as p1\n"
        + "right join DEPT as p2 on p1.empno = p2.deptno or p1.ename < p2.name";
    sql(sql).withRule(CoreRules.JOIN_EXPAND_OR_TO_UNION_RULE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6930">[CALCITE-6930]
   * Implementing JoinConditionOrExpansionRule</a>. */
  @Test void testJoinConditionOrExpansionRuleLeft() {
    String sql = "select *\n"
        + "from EMP as p1\n"
        + "left join EMP as p2 on p1.empno = p2.empno or p1.sal = p2.sal";
    sql(sql).withRule(CoreRules.JOIN_EXPAND_OR_TO_UNION_RULE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6930">[CALCITE-6930]
   * Implementing JoinConditionOrExpansionRule</a>. */
  @Test void testJoinConditionOrExpansionRuleAnti() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("DEPT")
        .scan("EMP")
        .antiJoin(
            b.or(
              b.equals(
                  b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")),
              b.equals(
                  b.field(2, 0, "DNAME"),
                  b.field(2, 1, "JOB"))))
        .build();
    relFn(relFn)
        .withRule(CoreRules.JOIN_EXPAND_OR_TO_UNION_RULE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6930">[CALCITE-6930]
   * Implementing JoinConditionOrExpansionRule</a>. */
  @Test void testJoinConditionOrExpansionRuleFull() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("DEPT")
        .scan("EMP")
        .join(JoinRelType.FULL,
            b.or(
                b.equals(
                    b.field(2, 0, "DEPTNO"),
                    b.field(2, 1, "DEPTNO")),
                b.equals(
                    b.field(2, 0, "DNAME"),
                    b.field(2, 1, "JOB"))))
        .build();
    relFn(relFn)
        .withRule(CoreRules.JOIN_EXPAND_OR_TO_UNION_RULE)
        .check();
  }

  @Test void testExchangeRemoveConstantKeysRule() {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .filter(
            b.call(
                SqlStdOperatorTable.EQUALS,
                b.field("EMPNO"),
                b.literal(10)))
        .exchange(RelDistributions.hash(ImmutableList.of(0)))
        .project(
            b.field(0),
            b.field(1))
        .sortExchange(
            RelDistributions.hash(ImmutableList.of(0, 1)),
            RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(1)))
        .build();

    relFn(relFn)
        .withRule(
            CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
            CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS)
        .check();
  }

  @Test void testReduceAverageWithNoReduceSum() {
    final RelOptRule rule = AggregateReduceFunctionsRule.Config.DEFAULT
        .withOperandFor(LogicalAggregate.class)
        .withFunctionsToReduce(EnumSet.of(SqlKind.AVG))
        .toRule();
    final String sql = "select name, max(name), avg(deptno), min(name)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testNoReduceAverage() {
    final RelOptRule rule = AggregateReduceFunctionsRule.Config.DEFAULT
        .withOperandFor(LogicalAggregate.class)
        .withFunctionsToReduce(EnumSet.noneOf(SqlKind.class))
        .toRule();
    String sql = "select name, max(name), avg(deptno), min(name)"
        + " from sales.dept group by name";
    sql(sql).withRule(rule).checkUnchanged();
  }

  @Test void testNoReduceSum() {
    final RelOptRule rule = AggregateReduceFunctionsRule.Config.DEFAULT
        .withOperandFor(LogicalAggregate.class)
        .withFunctionsToReduce(EnumSet.noneOf(SqlKind.class))
        .toRule();
    String sql = "select name, sum(deptno)"
            + " from sales.dept group by name";
    sql(sql).withRule(rule).checkUnchanged();
  }

  @Test void testReduceAverageAndVarWithNoReduceStddev() {
    // configure rule to reduce AVG and VAR_POP functions
    // other functions like SUM, STDDEV won't be reduced
    final RelOptRule rule = AggregateReduceFunctionsRule.Config.DEFAULT
        .withOperandFor(LogicalAggregate.class)
        .withFunctionsToReduce(EnumSet.of(SqlKind.AVG, SqlKind.VAR_POP))
        .toRule();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " var_pop(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testReduceAverageAndSumWithNoReduceStddevAndVar() {
    // configure rule to reduce AVG and SUM functions
    // other functions like VAR_POP, STDDEV_POP won't be reduced
    final RelOptRule rule = AggregateReduceFunctionsRule.Config.DEFAULT
        .withOperandFor(LogicalAggregate.class)
        .withFunctionsToReduce(EnumSet.of(SqlKind.AVG, SqlKind.SUM))
        .toRule();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " var_pop(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5000">[CALCITE-5000]
   * Expand rule of `AGGREGATE_REDUCE_FUNCTIONS`, when arg of agg-call exist in the agg's group</a>.
   */
  @Test void testReduceAggregateFunctionsByGroup() {
    final String sql = "select sal, max(sal) as sal_max, min(sal) as sal_min,\n"
        + "avg(sal) sal_avg, any_value(sal) as sal_val, first_value(sal) as sal_first,\n"
        + "last_value(sal) as sal_last\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS, CoreRules.PROJECT_MERGE).check();
  }

  @Test void testReduceAllAggregateFunctions() {
    // configure rule to reduce all used functions
    final RelOptRule rule = AggregateReduceFunctionsRule.Config.DEFAULT
        .withOperandFor(LogicalAggregate.class)
        .withFunctionsToReduce(
            EnumSet.of(SqlKind.AVG, SqlKind.SUM, SqlKind.STDDEV_POP,
                SqlKind.STDDEV_SAMP, SqlKind.VAR_POP, SqlKind.VAR_SAMP))
        .toRule();
    final String sql = "select name, stddev_pop(deptno), avg(deptno),"
        + " stddev_samp(deptno), var_pop(deptno), var_samp(deptno)\n"
        + "from sales.dept group by name";
    sql(sql).withRule(rule).check();
  }

  @Test void testReduceWithNonTypePredicate() {
    // Make sure we can reduce with more specificity than just agg function type.
    final RelOptRule rule = AggregateReduceFunctionsRule.Config.DEFAULT
        .withExtraCondition(call -> call.distinctKeys != null)
        .toRule();
    final String sql = "select avg(sal), avg(sal) within distinct (deptno)\n"
        + "from emp";
    sql(sql).withRule(rule).check();
  }

  /** Test case for
  * <a href="https://issues.apache.org/jira/browse/CALCITE-2803">[CALCITE-2803]
  * Identify expanded IS NOT DISTINCT FROM expression when pushing project past join</a>.
  */
  @Test void testPushProjectWithIsNotDistinctFromPastJoin() {
    final String sql = "select e.sal + b.comm from emp e inner join bonus b\n"
        + "on (e.ename || e.job) IS NOT DISTINCT FROM (b.ename || b.job) and e.deptno = 10";
    sql(sql)
        .withRelBuilderSimplify(false)
        .withRule(CoreRules.PROJECT_JOIN_TRANSPOSE)
        .check();
  }

  @Test void testDynamicStarWithUnion() {
    String sql = "(select n_nationkey from SALES.CUSTOMER)\n"
        + "union all\n"
        + "(select n_name from CUSTOMER_MODIFIABLEVIEW)";
    RuleSet ruleSet =
        RuleSets.ofList(EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE);
    sql(sql)
        .withVolcanoPlanner(false, p -> ruleSet.forEach(p::addRule))
        .withDynamicTable()
        .check();
  }

  @Test void testFilterAndProjectWithMultiJoin() {
    final HepProgram preProgram = new HepProgramBuilder()
        .addRuleCollection(Arrays.asList(MyFilterRule.INSTANCE, MyProjectRule.INSTANCE))
        .build();

    final FilterMultiJoinMergeRule filterMultiJoinMergeRule =
        FilterMultiJoinMergeRule.Config.DEFAULT
            .withOperandFor(MyFilter.class, MultiJoin.class)
            .toRule();

    final ProjectMultiJoinMergeRule projectMultiJoinMergeRule =
        ProjectMultiJoinMergeRule.Config.DEFAULT
            .withOperandFor(MyProject.class, MultiJoin.class)
            .toRule();

    HepProgram program = new HepProgramBuilder()
        .addRuleCollection(
            Arrays.asList(
                CoreRules.JOIN_TO_MULTI_JOIN,
                filterMultiJoinMergeRule,
                projectMultiJoinMergeRule))
        .build();

    sql("select * from emp e1 left outer join dept d on e1.deptno = d.deptno where d.deptno > 3")
        .withPre(preProgram).withProgram(program).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3151">[CALCITE-3151]
   * RexCall's Monotonicity is not considered in determining a Calc's
   * collation</a>. */
  @Test void testMonotonicityUDF() {
    final SqlFunction monotonicityFun =
        SqlBasicFunction.create("MONOFUN", ReturnTypes.BIGINT,
            OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION)
            .withDeterministic(false)
            .withMonotonicityInference(call -> SqlMonotonicity.INCREASING);

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
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();

    HepPlanner hepPlanner = new HepPlanner(hepProgram);
    hepPlanner.setRoot(root);
    final RelNode relAfter = hepPlanner.findBestExp();
    final RelCollation collationAfter =
        relAfter.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    assertThat(collationAfter, is(collationBefore));
  }

  @Test void testPushFilterWithIsNotDistinctFromPastJoin() {
    String sql = "SELECT * FROM "
        + "emp t1 INNER JOIN "
        + "emp t2 "
        + "ON t1.deptno = t2.deptno "
        + "WHERE t1.ename is not distinct from t2.ename";
    sql(sql).withRule(CoreRules.FILTER_INTO_JOIN).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3997">[CALCITE-3997]
   * Logical rules applied on physical operator but failed handle
   * traits</a>. */
  @Test void testMergeJoinCollation() {
    final String sql = "select r.ename, s.sal from\n"
        + "sales.emp r join sales.bonus s\n"
        + "on r.ename=s.ename where r.sal+1=s.sal";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRelTraitDef(RelCollationTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(p, false, false);
        })
        .check();
  }

  /**
   * Custom implementation of {@link Filter} for use
   * in test case to verify that {@link FilterMultiJoinMergeRule}
   * can be created with any {@link Filter} and not limited to
   * {@link org.apache.calcite.rel.logical.LogicalFilter}.
   */
  private static class MyFilter extends Filter {
    MyFilter(RelOptCluster cluster, RelTraitSet traitSet,
        RelNode input, RexNode condition) {
      super(cluster, traitSet, input, condition);
    }

    @Override public MyFilter copy(RelTraitSet traitSet, RelNode input,
        RexNode condition) {
      return new MyFilter(getCluster(), traitSet, input, condition);
    }
  }

  /**
   * Rule to transform {@link LogicalFilter} into
   * custom MyFilter.
   */
  public static class MyFilterRule extends RelRule<MyFilterRule.Config> {
    static final MyFilterRule INSTANCE = ImmutableMyFilterRuleConfig.builder()
        .build()
        .withOperandSupplier(b ->
            b.operand(LogicalFilter.class).anyInputs())
        .as(Config.class)
        .toRule();

    protected MyFilterRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalFilter logicalFilter = call.rel(0);
      final RelNode input = logicalFilter.getInput();
      final MyFilter myFilter =
          new MyFilter(input.getCluster(), input.getTraitSet(), input,
              logicalFilter.getCondition());
      call.transformTo(myFilter);
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(typeImmutable = "ImmutableMyFilterRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default MyFilterRule toRule() {
        return new MyFilterRule(this);
      }
    }
  }

  /**
   * Custom implementation of {@link Project} for use
   * in test case to verify that {@link ProjectMultiJoinMergeRule}
   * can be created with any {@link Project} and not limited to
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  private static class MyProject extends Project {
    MyProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType) {
      super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
    }

    public MyProject copy(RelTraitSet traitSet, RelNode input,
        List<RexNode> projects, RelDataType rowType) {
      return new MyProject(getCluster(), traitSet, input, projects, rowType);
    }
  }

  /**
   * Rule to transform {@link LogicalProject} into custom
   * MyProject.
   */
  public static class MyProjectRule
      extends RelRule<MyProjectRule.Config> {
    static final MyProjectRule INSTANCE = ImmutableMyProjectRuleConfig.builder().build()
        .withOperandSupplier(b -> b.operand(LogicalProject.class).anyInputs())
        .as(Config.class)
        .toRule();

    protected MyProjectRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final LogicalProject logicalProject = call.rel(0);
      final RelNode input = logicalProject.getInput();
      final MyProject myProject =
          new MyProject(input.getCluster(), input.getTraitSet(), input,
              logicalProject.getProjects(), logicalProject.getRowType());
      call.transformTo(myProject);
    }

    /** Rule configuration. */
    @Value.Immutable
    @Value.Style(typeImmutable = "ImmutableMyProjectRuleConfig")
    public interface Config extends RelRule.Config {
      @Override default MyProjectRule toRule() {
        return new MyProjectRule(this);
      }
    }
  }

  @Test void testSortJoinCopyInnerJoinOrderBy() {
    final String sql = "select * from sales.emp join sales.dept on\n"
        + "sales.emp.deptno = sales.dept.deptno order by sal";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_COPY)
        .check();
  }

  @Test void testSortJoinCopyInnerJoinOrderByLimit() {
    final String sql = "select * from sales.emp e join (\n"
        + "  select * from sales.dept d) d on e.deptno = d.deptno\n"
        + "order by sal limit 10";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_COPY)
        .check();
  }

  @Test void testSortJoinCopyInnerJoinOrderByTwoFields() {
    final String sql = "select * from sales.emp e join  sales.dept d on\n"
        + " e.deptno = d.deptno order by e.sal,d.name";
    sql(sql)
        .withPreRule(CoreRules.SORT_PROJECT_TRANSPOSE)
        .withRule(CoreRules.SORT_JOIN_COPY)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderBy() {
    final String sql = "select * from sales.dept d where d.deptno in\n"
        + " (select e.deptno from sales.emp e) order by d.deptno";
    sql(sql)
        .withExpand(true)
        .withPreRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .withRule(CoreRules.SORT_JOIN_COPY)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderByLimitOffset() {
    final String sql = "select * from sales.dept d where d.deptno in\n"
        + " (select e.deptno from sales.emp e) order by d.deptno limit 10 offset 2";
    // Do not copy the limit and offset
    sql(sql)
        .withExpand(true)
        .withPreRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .withRule(CoreRules.SORT_JOIN_COPY)
        .check();
  }

  @Test void testSortJoinCopySemiJoinOrderByOffset() {
    final String sql = "select * from sales.dept d where d.deptno in"
        + " (select e.deptno from sales.emp e) order by d.deptno offset 2";
    // Do not copy the offset
    sql(sql)
        .withExpand(true)
        .withPreRule(CoreRules.PROJECT_TO_SEMI_JOIN)
        .withRule(CoreRules.SORT_JOIN_COPY)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6824">[CALCITE-6824]
   * Subquery in join conditions rewrite fails if referencing a column
   * from the right-hand side table</a>. */
  @Test void testJoinSubQueryRemoveRuleWithNotIn() {
    final String sql = "SELECT empno FROM emp JOIN dept on "
        + "emp.deptno not in (SELECT deptno FROM dept)";
    sql(sql)
        .withRule(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6824">[CALCITE-6824]
   * Subquery in join conditions rewrite fails if referencing a column
   * from the right-hand side table</a>. */
  @Test void testJoinSubQueryRemoveRuleWithQuantifierSome() {
    final String sql = "SELECT empno FROM emp JOIN dept on "
        + "emp.deptno >= SOME(SELECT deptno FROM dept)";
    sql(sql)
        .withRule(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6824">[CALCITE-6824]
   * Subquery in join conditions rewrite fails if referencing a column
   * from the right-hand side table</a>. */
  @Test void testJoinSubQueryRewriteWithBothSidesColumns() {
    final String sql = "SELECT empno FROM emp JOIN dept on "
        + "emp.deptno + dept.deptno >= SOME(SELECT deptno FROM dept)";
    sql(sql)
        .withRule(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2295">[CALCITE-2295]
   * Correlated SubQuery with Project will generate error plan</a>. */
  @Test public void testDecorrelationWithProject() throws Exception {
    final String sql = "select sal,\n"
        + "exists (select * from emp_b where emp.deptno = emp_b.deptno)\n"
        + "from sales.emp";
    sql(sql).withSubQueryRules().withLateDecorrelate(true).check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3296">[CALCITE-3296]
   * Decorrelator gives empty result after decorrelating sort rel with
   * null offset and fetch</a>.
   */
  @Test void testDecorrelationWithSort() {
    final String sql = "SELECT e1.empno\n"
        + "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n"
        + "and e1.deptno < 10 and d1.deptno < 15\n"
        + "and e1.sal > (select avg(sal) from emp e2 where e1.empno = e2.empno)\n"
        + "order by e1.empno";

    sql(sql)
        .withRule() // empty program
        .withExpand(true)
        .withDecorrelate(true)
        .checkUnchanged();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3319">[CALCITE-3319]
   * AssertionError for ReduceDecimalsRule</a>. */
  @Test void testReduceDecimal() {
    final String sql = "select ename from emp where sal > cast (100.0 as decimal(4, 1))";
    sql(sql)
        .withRule(CoreRules.FILTER_TO_CALC,
            CoreRules.CALC_REDUCE_DECIMALS)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6870">[CALCITE-6870]
   * The FilterToCalcRule/ProjectToCalcRule should not convert a Filter/Project to Calc
   * when it contains Subquery</a>. */
  @Test void testFilterToCalc() {
    final String sql = "select ename from emp where sal > all (select comm from emp)";
    sql(sql)
        .withRule(CoreRules.FILTER_TO_CALC)
        .checkUnchanged();
  }

  @Test void testProjectToCalc() {
    final String sql = "select  sal > all (select comm from emp) from emp";
    sql(sql)
        .withRule(CoreRules.PROJECT_TO_CALC)
        .checkUnchanged();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6875">[CALCITE-6875]
   * EnumerableFilterRule/EnumerableProjectRule should not convert a Logical Filter/Project
   * to Enumerable Filter/Project when it contains Subquery</a>. */
  @Test void testEnumerableFilterRule() {
    final String sql = "select R_REGIONKEY from SALES.CUSTOMER\n"
        + "where R_REGIONKEY > all (select R_REGIONKEY from SALES.CUSTOMER)";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
          p.addRule(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
          p.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        })
        .withDynamicTable()
        .check();
  }

  @Test void testEnumerableProjectRule() {
    final String sql = "select R_REGIONKEY > all (select R_REGIONKEY from SALES.CUSTOMER)\n"
        + "from SALES.CUSTOMER";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
          p.addRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        })
        .withDynamicTable()
        .check();
  }

  @Test void testEnumerableCalcRule() {
    final String sql = "select FNAME, LNAME\n"
        + "from SALES.CUSTOMER\n"
        + "where CONTACTNO > 10";

    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
          p.addRule(CoreRules.FILTER_TO_CALC);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
        })
        .withDynamicTable()
        .withDecorrelate(true)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3404">[CALCITE-3404]
   * Treat agg expressions that can ignore distinct constraint as
   * distinct in AggregateExpandDistinctAggregatesRule when all the
   * other agg expressions are distinct and have same arguments</a>. */
  @Test void testMaxReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "max(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES).check();
  }

  @Test void testMinReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "min(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES).check();
  }

  @Test void testBitAndReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "bit_and(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES).check();
  }

  @Test void testBitOrReuseDistinctAttrWithMixedOptionality() {
    final String sql = "select sum(distinct deptno), count(distinct deptno), "
        + "bit_or(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES).check();
  }

  @Test void testProjectJoinTransposeItem() {
    ProjectJoinTransposeRule projectJoinTransposeRule =
        CoreRules.PROJECT_JOIN_TRANSPOSE.config
            .withOperandFor(Project.class, Join.class)
            .withPreserveExprCondition(RelOptRulesTest::skipItem)
            .toRule();

    final String sql = "select t1.c_nationkey[0], t2.c_nationkey[0]\n"
        + "from sales.customer as t1\n"
        + "left outer join sales.customer as t2\n"
        + "on t1.c_nationkey[0] = t2.c_nationkey[0]";

    sql(sql)
        .withDynamicTable()
        .withRule(projectJoinTransposeRule)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4317">[CALCITE-4317]
   * RelFieldTrimmer after trimming all the fields in an aggregate
   * should not return a zero field Aggregate</a>. */
  @Test void testProjectJoinTransposeRuleOnAggWithNoFieldsWithTrimmer() {
    fixture()
        .withVolcanoPlanner(false, p -> {
          p.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
          RelOptUtil.registerDefaultRules(p, false, false);
        })
        .withDynamicTable()
        .withTrim(true)
        .relFn(b -> {
          // Build a rel equivalent to sql:
          // SELECT name FROM (SELECT count(*) cnt_star, count(empno) cnt_en FROM sales.emp)
          // cross join sales.dept
          // limit 10

          RelNode left = b.scan("DEPT").build();
          RelNode right = b.scan("EMP")
              .project(b.alias(b.literal(0), "DUMMY"))
              .aggregate(b.groupKey(),
                  b.count(b.field(0)).as("DUMMY_COUNT"))
              .build();

          return b.push(left)
              .push(right)
              .join(JoinRelType.INNER, b.literal(true))
              .project(b.field("DEPTNO"))
              .build();
        })
        .withBefore((f, r) -> {
          final String planBeforeTrimming = NL + RelOptUtil.toString(r);
          f.diffRepos().assertEquals("planBeforeTrimming",
              "${planBeforeTrimming}", planBeforeTrimming);

          RelNode r2 = f.tester.trimRelNode(f.factory, r);
          final String planAfterTrimming = NL + RelOptUtil.toString(r2);
          f.diffRepos().assertEquals("planAfterTrimming",
              "${planAfterTrimming}", planAfterTrimming);
          return r2;
        })
        .withRule(CoreRules.PROJECT_JOIN_TRANSPOSE)
        .checkUnchanged();
  }

  @Test void testSimplifyItemIsNotNull() {
    final String sql = "select *\n"
        + "from sales.customer as t1\n"
        + "where t1.c_nationkey[0] is not null";

    sql(sql)
        .withDynamicTable()
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  @Test void testSimplifyItemIsNull() {
    String sql = "select * from sales.customer as t1 where t1.c_nationkey[0] is null";

    sql(sql)
        .withDynamicTable()
        .withRule(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .checkUnchanged();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5247">[CALCITE-5247]
   * FilterJoinRule cannot simplify left join to inner join for `WHERE
   * RHS.C1 IS NOT NULL OR RHS.C2 IS NOT NULL`</a>.
   *
   * <p>This tests the case where the condition contains an OR between
   * the IS NOT NULL filters. */
  @Test void testFilterJoinRuleOrIsNotNull() {
    final String sql = "select * from\n"
        + "emp LHS\n"
        + "left join dept RHS on LHS.EMPNO = RHS.DEPTNO\n"
        + "where\n"
        + "RHS.DEPTNO is not null\n"
        + "OR RHS.NAME is not null";

    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5247">[CALCITE-5247]
   * FilterJoinRule cannot simplify left join to inner join for `WHERE
   * RHS.C1 IS NOT NULL OR RHS.C2 IS NOT NULL`</a>.
   *
   * <p>This tests the case where the condition contains an AND
   * between the IS NOT NULL filters. */
  @Test void testFilterJoinRuleAndIsNotNull() {
    final String sql = "select * from\n"
        + "emp LHS\n"
        + "left join dept RHS on LHS.EMPNO = RHS.DEPTNO\n"
        + "where\n"
        + "RHS.DEPTNO is not null\n"
        + "AND RHS.NAME is not null";

    sql(sql)
        .withRule(CoreRules.FILTER_INTO_JOIN)
        .check();
  }

  @Test void testJoinCommuteRuleWithAlwaysTrueConditionAllowed() {
    checkJoinCommuteRuleWithAlwaysTrueConditionDisallowed(true);
  }

  @Test void testJoinCommuteRuleWithAlwaysTrueConditionDisallowed() {
    checkJoinCommuteRuleWithAlwaysTrueConditionDisallowed(false);
  }

  private void checkJoinCommuteRuleWithAlwaysTrueConditionDisallowed(boolean allowAlwaysTrue) {
    final Function<RelBuilder, RelNode> relFn = b -> b
        .scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER,
            b.literal(true))
        .build();

    JoinCommuteRule.Config ruleConfig = JoinCommuteRule.Config.DEFAULT;
    if (!allowAlwaysTrue) {
      ruleConfig = ruleConfig.withAllowAlwaysTrueCondition(false);
    }

    HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addRuleInstance(ruleConfig.toRule())
        .build();
    HepPlanner hepPlanner = new HepPlanner(program);

    if (allowAlwaysTrue) {
      relFn(relFn).withPlanner(hepPlanner).check();
    } else {
      relFn(relFn).withPlanner(hepPlanner).checkUnchanged();
    }
  }

  @Test void testJoinAssociateRuleWithBottomAlwaysTrueConditionAllowed() {
    checkJoinAssociateRuleWithBottomAlwaysTrueCondition(true);
  }

  @Test void testJoinAssociateRuleWithBottomAlwaysTrueConditionDisallowed() {
    checkJoinAssociateRuleWithBottomAlwaysTrueCondition(false);
  }

  private void checkJoinAssociateRuleWithBottomAlwaysTrueCondition(boolean allowAlwaysTrue) {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode bottomLeft = b.scan("EMP").build();
      RelNode bottomRight = b.scan("DEPT").build();
      RelNode top = b.scan("BONUS").build();

      return b.push(bottomLeft)
          .push(bottomRight)
          .join(JoinRelType.INNER,
              b.equals(b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .push(top)
          .join(JoinRelType.INNER,
              b.equals(b.field(2, 0, "JOB"),
                  b.field(2, 1, "JOB")))
          .build();
    };

    JoinAssociateRule.Config ruleConfig = JoinAssociateRule.Config.DEFAULT;
    if (!allowAlwaysTrue) {
      ruleConfig = ruleConfig.withAllowAlwaysTrueCondition(false);
    }

    HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addMatchOrder(HepMatchOrder.TOP_DOWN)
        .addRuleInstance(ruleConfig.toRule())
        .build();
    HepPlanner hepPlanner = new HepPlanner(program);

    if (allowAlwaysTrue) {
      relFn(relFn).withPlanner(hepPlanner).check();
    } else {
      relFn(relFn).withPlanner(hepPlanner).checkUnchanged();
    }
  }

  @Test void testJoinAssociateRuleWithTopAlwaysTrueConditionAllowed() {
    checkJoinAssociateRuleWithTopAlwaysTrueCondition(true);
  }

  @Test void testJoinAssociateRuleWithTopAlwaysTrueConditionDisallowed() {
    checkJoinAssociateRuleWithTopAlwaysTrueCondition(false);
  }

  private void checkJoinAssociateRuleWithTopAlwaysTrueCondition(boolean allowAlwaysTrue) {
    final Function<RelBuilder, RelNode> relFn = b -> {

      RelNode bottomLeft = b.scan("EMP").build();
      RelNode bottomRight = b.scan("BONUS").build();
      RelNode top = b.scan("DEPT").build();

      return b.push(bottomLeft)
          .push(bottomRight)
          .join(JoinRelType.INNER,
              b.literal(true))
          .push(top)
          .join(JoinRelType.INNER,
              b.equals(b.field(2, 0, "DEPTNO"),
                  b.field(2, 1, "DEPTNO")))
          .build();
    };

    JoinAssociateRule.Config ruleConfig = JoinAssociateRule.Config.DEFAULT;
    if (!allowAlwaysTrue) {
      ruleConfig = ruleConfig.withAllowAlwaysTrueCondition(false);
    }

    HepProgram program = new HepProgramBuilder()
        .addMatchLimit(1)
        .addMatchOrder(HepMatchOrder.TOP_DOWN)
        .addRuleInstance(ruleConfig.toRule())
        .build();
    HepPlanner hepPlanner = new HepPlanner(program);

    if (allowAlwaysTrue) {
      relFn(relFn).withPlanner(hepPlanner).check();
    } else {
      relFn(relFn).withPlanner(hepPlanner).checkUnchanged();
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4652">[CALCITE-4652]
   * AggregateExpandDistinctAggregatesRule must cast top aggregates to original
   * type</a>.
   *
   * <p>Checks AggregateExpandDistinctAggregatesRule when return type of the SUM
   * aggregate is changed (expanded) by define custom type factory. */
  @Test void testDistinctCountWithExpandSumType() {
    // Define new type system to expand SUM return type.
    RelDataTypeSystemImpl typeSystem = new RelDataTypeSystemImpl() {
      @Override public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
          RelDataType argumentType) {
        switch (argumentType.getSqlTypeName()) {
        case INTEGER:
        case BIGINT:
          return typeFactory.createSqlType(SqlTypeName.DECIMAL);

        default:
          return super.deriveSumType(typeFactory, argumentType);
        }
      }
    };

    // Expected plan:
    // LogicalProject(EXPR$0=[CAST($0):BIGINT NOT NULL], EXPR$1=[$1])
    //   LogicalAggregate(group=[{}], EXPR$0=[$SUM0($1)], EXPR$1=[COUNT($0)])
    //     LogicalAggregate(group=[{0}], EXPR$0=[COUNT()])
    //       LogicalProject(COMM=[$6])
    //         LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //
    // The top 'LogicalProject' must be added in case SUM type is expanded
    // because type of original expression 'COUNT(DISTINCT comm)' is BIGINT
    // and type of SUM (of BIGINT) is DECIMAL.
    sql("SELECT count(comm), COUNT(DISTINCT comm) FROM emp")
        .withFactory(f -> f.withTypeSystem(ignore -> typeSystem))
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4818">[CALCITE-4818]
   * AggregateExpandDistinctAggregatesRule must infer correct data type for top
   * aggregate calls</a>.
   *
   * <p>Checks AggregateExpandDistinctAggregatesRule when return type of the SUM
   * aggregate is changed (expanded) by define custom type factory. */
  @Test void testSumAndDistinctSumWithExpandSumType() {
    // Define new type system to expand SUM return type.
    RelDataTypeSystemImpl typeSystem = new RelDataTypeSystemImpl() {
      @Override public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
          RelDataType argumentType) {
        switch (argumentType.getSqlTypeName()) {
        case INTEGER:
          return typeFactory.createSqlType(SqlTypeName.BIGINT);
        case BIGINT:
          return typeFactory.createSqlType(SqlTypeName.DECIMAL);

        default:
          return super.deriveSumType(typeFactory, argumentType);
        }
      }
    };

    // Expected plan:
    // LogicalProject(EXPR$0=[CAST($0):BIGINT], EXPR$1=[$1])
    //  LogicalAggregate(group=[{}], EXPR$0=[SUM($1)], EXPR$1=[SUM($0)]) // RowType[DECIMAL, BIGINT]
    //    LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])  // RowType[INTEGER, BIGINT]
    //      LogicalProject(COMM=[$6])
    //        LogicalTableScan(table=[[CATALOG, SALES, EMP]])
    //
    // The top 'LogicalProject' must be added in case SUM type is expanded
    // because type of original expression 'COUNT(DISTINCT comm)' is BIGINT
    // and type of SUM (of BIGINT) is DECIMAL.
    sql("SELECT SUM(comm), SUM(DISTINCT comm) FROM emp")
        .withFactory(f -> f.withTypeSystem(ignore -> typeSystem))
        .withRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        .check();
  }

  @Test void testLoptOptimizeJoinRulePrioritizeSelfJoin() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();

    sql("select e.empno from emp e"
        + " inner join dept d on d.deptno = e.deptno"
        + " inner join emp e2 on e.empno = e2.empno")
        .withPre(program)
        .withRule(CoreRules.MULTI_JOIN_OPTIMIZE)
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6788">[CALCITE-6788]
   * LoptOptimizeJoinRule should be able to delegate costs to the planner</a>. */
  @Test void testLoptOptimizeJoinRuleWithDefaultCost() {
    // Use the default rule
    checkLoptOptimizeJoinRule(CoreRules.MULTI_JOIN_OPTIMIZE);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6788">[CALCITE-6788]
   * LoptOptimizeJoinRule should be able to delegate costs to the planner</a>. */
  @Test void testLoptOptimizeJoinRuleWithSpecialCost() {
    // Use an ad-hoc version of the rule that uses planner#getCost instead of mq#getCumulativeCost
    checkLoptOptimizeJoinRule(LoptOptimizeJoinRule.Config.DEFAULT
        .withCostFunction((c, r) -> c.getPlanner().getCost(r, c.getMetadataQuery()))
        .toRule());
  }

  private void checkLoptOptimizeJoinRule(LoptOptimizeJoinRule rule) {
    final HepProgram preProgram = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .build();

    final HepProgram program = HepProgram.builder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(rule)
        .build();

    // Special planner that artificially favors joins on the same table
    final HepPlanner planner = new HepPlanner(program) {
      @Override public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
        if (rel instanceof Join
            && rel.getInput(0).stripped() instanceof TableScan
            && rel.getInput(1).stripped() instanceof TableScan) {
          TableScan left = (TableScan) rel.getInput(0).stripped();
          TableScan right = (TableScan) rel.getInput(1).stripped();
          if (left.getTable().equals(right.getTable())) {
            // Tiny cost for self-joins
            return getCostFactory().makeTinyCost();
          }
        }

        // General case: just define a kind of cumulative cost based on the rowCount (to avoid
        // the infinite costs from the Logical operators)
        RelOptCost cost = new RelOptCostImpl(mq.getRowCount(rel));
        for (RelNode input : rel.getInputs()) {
          cost = cost.plus(getCost(input, mq));
        }
        return cost;
      }
    };

    sql("select e1.empno from emp e1"
        + " inner join dept d1 on d1.deptno = e1.deptno"
        + " inner join emp e2 on e1.ename = e2.ename"
        + " inner join dept d2 on d2.deptno = e1.deptno")
        .withPre(preProgram)
        .withPlanner(planner)
        .check();
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6874">[CALCITE-6874]
   * FilterCalcMergeRule/ProjectCalcMergeRule should not merge a Filter/Project to Calc
   * when it contains Subquery</a>. */
  @Test void testFilterCalcMergeRule() {
    final String sql = "select deptno from sales.emp where\n"
        + "exists (select deptno from sales.emp where empno < 20)\n";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .addRuleInstance(CoreRules.PROJECT_TO_CALC)
        .build();
    sql(sql)
        .withPre(program)
        .withRule(CoreRules.FILTER_CALC_MERGE)
        .checkUnchanged();
  }

  @Test void testProjectCalcMergeRule() {
    final String sql = "select exists (select deptno from sales.emp)\n"
        + "from (select deptno from sales.emp where empno < 20)\n";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_TO_CALC)
        .build();
    sql(sql)
        .withPre(program)
        .withRule(CoreRules.PROJECT_CALC_MERGE)
        .checkUnchanged();
  }

  /**
   * Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6850">[CALCITE-6850]
   * ProjectRemoveRule with two Projects
   * does not keep field names from the top one</a>. */
  @Test void testProjectRemoveRule() {
    final Function<RelBuilder, RelNode> relFn = b -> {
      RelNode r = b.scan("DEPT")
          .project(
              b.call(SqlStdOperatorTable.PLUS,
              ImmutableList.of(b.field(0), b.literal(1))))
          .build();
      b.push(r);
      return LogicalProject.create(r, ImmutableList.of(),
          ImmutableList.of(b.field(0)),
          ImmutableList.of("newAlias"), ImmutableSet.of());
    };
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(CoreRules.PROJECT_REMOVE);
    HepPlanner planner =
        new HepPlanner(builder.build(), null, true, null, RelOptCostImpl.FACTORY);
    fixture().withRelBuilderConfig(a -> a.withBloat(-1))
        .relFn(relFn).withPlanner(planner).check();
  }

  @Test void testChainJoinDphypJoinReorder() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from "
        + "emp, emp_address, dept, dept_nested "
        + "where emp.deptno + emp_address.empno = dept.deptno + dept_nested.deptno "
        + "and emp.empno = emp_address.empno "
        + "and dept.deptno = dept_nested.deptno")
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testStarJoinDphypJoinReorder() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from "
        + "emp, emp_b, emp_address, dept, dept_nested "
        + "where emp.empno = emp_b.empno "
        + "and emp.empno = emp_address.empno "
        + "and emp.deptno = dept.deptno "
        + "and emp.deptno = dept_nested.deptno "
        + "and emp_b.sal + emp_address.empno = dept.deptno + dept_nested.deptno")
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testCycleJoinDphypJoinReorder() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from "
        + "emp, emp_b, dept, dept_nested "
        + "where emp.empno = emp_b.empno "
        + "and emp_b.deptno = dept.deptno "
        + "and dept.name = dept_nested.name "
        + "and dept_nested.deptno = emp.deptno "
        + "and emp.sal + emp_b.sal = dept.deptno + dept_nested.deptno")
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  /**
   * Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6836">[CALCITE-6836]
   * Add Rule to convert INTERSECT to EXISTS</a>. */
  @Test void testIntersectToExistsRuleOneField() {
    String sql = "SELECT a.ename FROM emp AS a\n"
        + "INTERSECT\n"
        + "SELECT b.name FROM dept AS b";
    sql(sql).withRule(CoreRules.INTERSECT_TO_EXISTS)
        .check();
  }

  /**
   * Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6836">[CALCITE-6836]
   * Add Rule to convert INTERSECT to EXISTS</a>. */
  @Test void testIntersectToExistsRulePrimaryKey() {
    String sql = "SELECT a.empno FROM emp AS a\n"
        + "INTERSECT\n"
        + "SELECT b.empno FROM emp AS b";
    sql(sql).withRule(CoreRules.INTERSECT_TO_EXISTS)
        .check();
  }

  /**
   * Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6836">[CALCITE-6836]
   * Add Rule to convert INTERSECT to EXISTS</a>. */
  @Test void testIntersectToExistsRuleMultiFields() {
    String sql = "SELECT a.ename, a.job FROM emp AS a\n"
        + "INTERSECT\n"
        + "SELECT b.ename, b.job FROM emp AS b";
    sql(sql).withRule(CoreRules.INTERSECT_TO_EXISTS)
        .check();
  }

  /**
   * Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6836">[CALCITE-6836]
   * Add Rule to convert INTERSECT to EXISTS</a>. */
  @Test void testIntersectToExistsRuleMultiIntersect() {
    String sql = "SELECT a.ename FROM emp AS a\n"
        + "INTERSECT\n"
        + "SELECT b.name FROM dept AS b\n"
        + "INTERSECT\n"
        + "SELECT c.ename FROM emp AS c";
    sql(sql).withRule(CoreRules.INTERSECT_TO_EXISTS)
        .check();
  }

  /**
   * Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6836">[CALCITE-6836]
   * Add Rule to convert INTERSECT to EXISTS</a>. */
  @Test void testIntersectToExistsRuleWithAll() {
    String sql = "SELECT a.ename FROM emp AS a\n"
        + "INTERSECT ALL\n"
        + "SELECT b.name FROM dept AS b";
    sql(sql).withRule(CoreRules.INTERSECT_TO_EXISTS)
        .checkUnchanged();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6891">[CALCITE-6891]
   * Implement IntersectReorderRule</a>. */
  @Test void testIntersectReorderRule() {
    final String sql = "select deptno from emp\n"
        + "intersect\n"
        + "select deptno from emp where deptno > 5\n";
    sql(sql).withVolcanoPlanner(true, p -> {
      p.addRule(CoreRules.INTERSECT_REORDER);
      p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    }).check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6891">[CALCITE-6891]
   * Implement IntersectReorderRule</a>. */
  @Test void testIntersectReorderRuleSameRowCount() {
    final String sql = "select deptno from emp where deptno > 10\n"
        + "intersect\n"
        + "select deptno from emp where deptno > 5\n";
    sql(sql).withVolcanoPlanner(true, p -> {
      p.addRule(CoreRules.INTERSECT_REORDER);
      p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    }).check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6891">[CALCITE-6891]
   * Implement IntersectReorderRule</a>. */
  @Test void testIntersectReorderRuleAll() {
    final String sql = "select deptno from emp\n"
        + "intersect all\n"
        + "select deptno from emp where deptno > 5\n";
    sql(sql).withVolcanoPlanner(true, p -> {
      p.addRule(CoreRules.INTERSECT_REORDER);
      p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
      p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    }).check();
  }

  @Test void testExpandFilterDisjunctionForTable() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.EXPAND_FILTER_DISJUNCTION_GLOBAL)
        .build();

    sql("select e.empno from emp as e, empnullables as en "
        + "where e.empno = en.empno "
        + "and "
        + "( "
        + "  (e.mgr > 100 and en.sal < 200) "
        + "  or "
        + "  e.comm < 50 "
        + ") ")
        .withPre(program)
        .withRule(CoreRules.FILTER_INTO_JOIN)
        .check();
  }

  @Test void testExpandJoinDisjunctionForTable() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.EXPAND_JOIN_DISJUNCTION_GLOBAL)
        .build();

    sql("select e.empno from emp as e inner join empnullables as en "
        + "on e.empno = en.empno "
        + "and "
        + "( "
        + "  (e.mgr > 100 and en.sal < 200) "
        + "  or "
        + "  (e.comm < 50 and en.deptno > 10) "
        + ") ")
        .withPre(program)
        .withRule(CoreRules.JOIN_CONDITION_PUSH)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6973">[CALCITE-6973]
   * Add rule for convert Minus to Filter</a>. */
  @Test void testMinusToFilterRuleWithTwoFilters() {
    final String sql = "SELECT mgr, comm FROM emp WHERE mgr = 12\n"
        + "EXCEPT\n"
        + "SELECT mgr, comm FROM emp WHERE comm = 5\n";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.MINUS_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6973">[CALCITE-6973]
   * Add rule for convert Minus to Filter</a>. */
  @Test void testMinusToFilterRuleWithOneFilter() {
    final String sql = "SELECT mgr, comm FROM emp\n"
        + "EXCEPT\n"
        + "SELECT mgr, comm FROM emp WHERE comm = 5\n";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.MINUS_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testMinusToFilterRuleNWayWithMultiSources() {
    final String sql = "SELECT deptno FROM emp WHERE deptno > 6\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM dept WHERE deptno > 8\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM emp WHERE deptno > 12\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM dept WHERE deptno > 10\n";
    sql(sql)
        .withPreRule(CoreRules.MINUS_MERGE, CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.MINUS_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testMinusToFilterRuleFourWayWithForSources() {
    final String sql = "SELECT deptno FROM emp WHERE deptno = 12\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM dept WHERE deptno = 5\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM empnullables WHERE deptno = 6\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM deptnullables WHERE deptno = 10\n";
    sql(sql)
        .withPreRule(CoreRules.MINUS_MERGE, CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.MINUS_FILTER_TO_FILTER)
        .checkUnchanged();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testMinusToFilterNWayWithSubquery() {
    final String sql = "SELECT deptno FROM emp WHERE deptno > 12\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM dept WHERE deptno = 5\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM emp e1 WHERE EXISTS (\n"
        + "    SELECT 1 FROM emp e2\n"
        + "    WHERE e2.comm = e1.comm)\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM dept WHERE deptno = 10\n"
        + "EXCEPT\n"
        + "SELECT deptno FROM emp WHERE deptno > 20\n";
    sql(sql)
        .withPreRule(CoreRules.MINUS_MERGE, CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.MINUS_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testUnionToFilterRuleWithTwoFilters() {
    final String sql = "SELECT mgr, comm FROM emp WHERE mgr = 12\n"
        + "UNION\n"
        + "SELECT mgr, comm FROM emp WHERE comm = 5\n";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.UNION_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testUnionToFilterRuleWithOneFilter() {
    final String sql = "SELECT mgr, comm FROM emp\n"
        + "UNION\n"
        + "SELECT mgr, comm FROM emp WHERE comm = 5\n";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.UNION_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testUnionToFilterRuleNWayWithMultiSources() {
    final String sql = "SELECT deptno FROM emp WHERE deptno = 12\n"
        + "UNION\n"
        + "SELECT deptno FROM dept WHERE deptno = 5\n"
        + "UNION\n"
        + "SELECT deptno FROM emp WHERE deptno = 6\n"
        + "UNION\n"
        + "SELECT deptno FROM dept WHERE deptno = 10\n";
    sql(sql)
        .withPreRule(CoreRules.UNION_MERGE, CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.UNION_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testIntersectToFilterRuleWithTwoFilters() {
    final String sql = "SELECT mgr, comm FROM emp WHERE mgr = 12\n"
        + "INTERSECT\n"
        + "SELECT mgr, comm FROM emp WHERE comm = 5\n";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.INTERSECT_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testIntersectToFilterRuleWithOneFilter() {
    final String sql = "SELECT mgr, comm FROM emp\n"
        + "INTERSECT\n"
        + "SELECT mgr, comm FROM emp WHERE comm = 5\n";
    sql(sql)
        .withPreRule(CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.INTERSECT_FILTER_TO_FILTER)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7002">[CALCITE-7002]
   * Create an optimization rule to eliminate UNION
   * from the same source with different filters</a>. */
  @Test void testIntersectToFilterRuleNWayWithMultiSources() {
    final String sql = "SELECT deptno FROM emp WHERE deptno < 12\n"
        + "INTERSECT\n"
        + "SELECT deptno FROM dept WHERE deptno > 5\n"
        + "INTERSECT\n"
        + "SELECT deptno FROM emp WHERE deptno > 6\n"
        + "INTERSECT\n"
        + "SELECT deptno FROM dept WHERE deptno < 10\n";
    sql(sql)
        .withPreRule(CoreRules.INTERSECT_MERGE, CoreRules.PROJECT_FILTER_TRANSPOSE)
        .withRule(CoreRules.INTERSECT_FILTER_TO_FILTER)
        .check();
  }

  @Test void testExpandFilterDisjunctionForJoinInput() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.EXPAND_FILTER_DISJUNCTION_LOCAL)
        .build();

    sql("select e.empno from emp as e, empnullables as en, emp_b as eb\n"
        + "where e.empno = en.empno and e.empno = eb.empno\n"
        + "and\n"
        + "(\n"
        + "  (e.mgr > 100 and eb.sal < 200)\n"
        + "  or\n"
        + "  (en.comm < 50 and eb.deptno > 10)\n"
        + ")")
        .withPre(program)
        .withRule(CoreRules.JOIN_CONDITION_PUSH, CoreRules.FILTER_INTO_JOIN)
        .check();
  }

  @Test void testExpandJoinDisjunctionForJoinInput() {
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.EXPAND_JOIN_DISJUNCTION_LOCAL)
        .build();

    sql("select e.empno from emp as e inner join empnullables as en\n"
        + " on e.empno = en.empno\n"
        + " inner join emp_b as eb\n"
        + " on e.empno = eb.empno and\n"
        + "(\n"
        + "  (e.mgr > 100 and eb.sal < 200)\n"
        + "  or\n"
        + "  (en.comm < 50 and eb.deptno > 10)\n"
        + ")")
        .withPre(program)
        .withRule(CoreRules.JOIN_CONDITION_PUSH, CoreRules.FILTER_INTO_JOIN)
        .check();
  }

  @Test void testAggregateMinMaxToLimitRule() {
    final String sql = "select min(deptno), max(deptno) from emp";
    sql(sql).withRule(CoreRules.AGGREGATE_MIN_MAX_TO_LIMIT)
        .check();
  }

  @Test void testOuterJoinForDphyp() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from "
        + "emp_address inner join emp on emp_address.empno = emp.empno "
        + "left join dept on emp.deptno = dept.deptno "
        + "inner join dept_nested on dept.deptno = dept_nested.deptno")
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testSemiJoinForDphyp() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from emp, emp_address, salgrade "
        + "where emp_address.empno = emp.empno and emp.sal = salgrade.hisal "
        + "and exists(select * from emp_b where emp_address.empno = emp_b.empno)")
        .withExpand(true)
        .withDecorrelate(true)
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testCartesianProductForDphyp() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from emp_address, emp, emp_b, dept "
        + "where emp_address.empno = emp.empno "
        + "and dept.deptno = emp.deptno")
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testComplexPredicateForDphyp() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from "
        + "emp, emp_b, dept, dept_nested "
        + "where emp.deptno + emp_b.empno = dept.deptno + dept_nested.deptno "
        + "and (emp.empno = emp_b.empno or emp.ename = emp_b.ename) "
        + "and sqrt(dept.deptno + dept_nested.deptno) > 2")
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testInnerLeftSemiJoinTypeForDphyp() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from "
        + "emp_address inner join emp on emp_address.empno = emp.empno "
        + "left join dept on emp.deptno = dept.deptno "
        + "where exists(select * from dept_nested where dept.deptno = dept_nested.deptno)")
        .withExpand(true)
        .withDecorrelate(true)
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testLeftInnerSemiJoinTypeForDphyp() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from "
        + "emp_address left join emp on emp_address.empno = emp.empno "
        + "inner join dept on emp.deptno = dept.deptno "
        + "where exists(select * from dept_nested where dept.deptno = dept_nested.deptno)")
        .withExpand(true)
        .withDecorrelate(true)
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  @Test void testFullLeftSemiJoinTypeForDphyp() {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_TO_SEMI_JOIN)
        .addRuleInstance(CoreRules.JOIN_TO_HYPER_GRAPH)
        .build();

    sql("select emp.empno from "
        + "emp_address full join emp on emp_address.empno = emp.empno "
        + "left join dept on emp.deptno = dept.deptno "
        + "where exists(select * from dept_nested where dept.deptno = dept_nested.deptno)")
        .withExpand(true)
        .withDecorrelate(true)
        .withPre(program)
        .withRule(CoreRules.HYPER_GRAPH_OPTIMIZE, CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_MERGE)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7047">[CALCITE-7047]
   * Improve Volcano planner selection of sort conversion rules</a>. */
  @Test void testLimitSortOnlyLimit() {
    String sql = "select * from emp limit 10";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        })
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7047">[CALCITE-7047]
   * Improve Volcano planner selection of sort conversion rules</a>. */
  @Test void testLimitSortOnlyOffset() {
    String sql = "select * from emp offset 10";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        })
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7047">[CALCITE-7047]
   * Improve Volcano planner selection of sort conversion rules</a>. */
  @Test void testLimitSortOnlyOrderBy() {
    String sql = "select * from emp order by deptno";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        })
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7047">[CALCITE-7047]
   * Improve Volcano planner selection of sort conversion rules</a>. */
  @Test void testLimitSortOrderByLimit() {
    String sql = "select * from emp order by deptno limit 10";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        })
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7047">[CALCITE-7047]
   * Improve Volcano planner selection of sort conversion rules</a>. */
  @Test void testLimitSortOrderByOffsetLimit() {
    String sql = "select * from emp order by deptno limit 10 offset 5";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        })
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7047">[CALCITE-7047]
   * Improve Volcano planner selection of sort conversion rules</a>. */
  @Test void testLimitSortOrderByOffset() {
    String sql = "select * from emp order by deptno offset 5";
    sql(sql)
        .withVolcanoPlanner(false, p -> {
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
          p.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        })
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7077">[CALCITE-7077]
   * Implement a rule to rewrite FULL JOIN as LEFT JOIN and RIGHT JOIN</a>. */
  @Test void testFullJoinToLeftAndRightJoin() {
    final String query = "select * from emp e1\n"
        + "full join emp e2\n"
        + "on e1.sal = e2.sal and e1.mgr is null";
    sql(query)
        .withRule(CoreRules.FULL_TO_LEFT_AND_RIGHT_JOIN)
        .check();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7077">[CALCITE-7077]
   * Implement a rule to rewrite FULL JOIN as LEFT JOIN and RIGHT JOIN</a>. */
  @Test void testFullJoinToLeftAndRightJoinNonDeterministic() {
    final String query = "select * from emp e1\n"
        + "full join emp e2\n"
        + "on rand() > 0.5";
    sql(query)
        .withRule(CoreRules.FULL_TO_LEFT_AND_RIGHT_JOIN)
        .checkUnchanged();
  }

  /** Test case of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7086">[CALCITE-7086]
   * Implement a rule that performs the inverse operation of AggregateCaseToFilterRule</a>. */
  @Test void testAggregateFilterToCase() {
    final String sql = "select coalesce(sum_match, 0) as sum0_match, sum_distinct_not_match,\n"
        + " count_distinct_match, count_star_match from (\n"
        + " select\n"
        + " sum(sal) filter(where deptno = 10) as sum_match,\n"
        + " sum(distinct empno) filter(where deptno = 20) as sum_distinct_not_match,\n"
        + " count(distinct deptno) filter(where job = 'CLERK') as count_distinct_match,\n"
        + " count(*) filter(where deptno = 40) as count_star_match\n"
        + " from emp\n"
        + " )";
    HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.PROJECT_AGGREGATE_MERGE)
        .build();

    sql(sql)
        .withPre(program)
        .withRule(CoreRules.AGGREGATE_FILTER_TO_CASE)
        .check();
  }
}
