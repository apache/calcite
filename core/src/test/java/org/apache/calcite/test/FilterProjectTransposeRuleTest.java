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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link FilterProjectTransposeRule}.
 */
class FilterProjectTransposeRuleTest {

  private static RelOptFixture fixture() {
    return RelOptFixture.DEFAULT.withDiffRepos(
        DiffRepository.lookup(FilterProjectTransposeRuleTest.class));
  }

  private static RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  private static RelOptFixture relFn(Function<RelBuilder, RelNode> relFn) {
    return fixture().relFn(relFn);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7939">[CALCITE-7939]
   * Non-flat AND/OR conditions cause AssertionError in Filter when created by
   * FilterProjectTransposeRule or RelBuilder.filter</a>. */
  @Test void testFilterProjectTransposeBloatCountsFlattenedCondition() {
    final Function<RelBuilder, RelNode> relFn = b -> b.scan("EMP")
        .project(
            b.alias(
                b.or(b.greaterThan(b.field("SAL"), b.literal(1000)),
                    b.greaterThan(b.field("COMM"), b.literal(100))),
                "P"),
            b.field("EMPNO"))
        .filter(
            b.and(
                b.or(b.field("P"),
                    b.greaterThan(b.field("EMPNO"), b.literal(7000))),
                b.or(b.field("P"),
                    b.lessThan(b.field("EMPNO"), b.literal(100)))))
        .build();
    relFn(relFn)
        .withRule(CoreRules.FILTER_PROJECT_TRANSPOSE.config
            .withBloat(2).toRule())
        .check();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7939">[CALCITE-7939]
   * Non-flat AND/OR conditions cause AssertionError in Filter when created by
   * FilterProjectTransposeRule or RelBuilder.filter</a>. */
  @Test void testFilterProjectTransposeNonFlatOrUnderAnd() {
    final String sql = "select * from (\n"
        + "  select sal > 1000 or comm > 100 as p, empno from emp)\n"
        + "where (p or empno > 7000) and empno < 9000";
    sql(sql).withRule(CoreRules.FILTER_PROJECT_TRANSPOSE).check();
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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7854">[CALCITE-7854]
   * FilterProjectTransposeRule throws NullPointerException when the Filter has
   * a distribution trait and the Project's input has none</a>.
   *
   * <p>The pushed Filter gets the default distribution. The rule used to put
   * a null distribution into the new trait set and failed with
   * NullPointerException. */
  @Test void testFilterProjectTransposeRuleInputWithoutDistribution() {
    assertThat(pushedFilterDistribution(null), is(RelDistributions.ANY));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7854">[CALCITE-7854]
   * FilterProjectTransposeRule throws NullPointerException when the Filter has
   * a distribution trait and the Project's input has none</a>.
   *
   * <p>The pushed Filter takes the input's distribution, not the one the
   * original Filter had. */
  @Test void testFilterProjectTransposeRuleInputWithDistribution() {
    assertThat(pushedFilterDistribution(RelDistributions.SINGLETON),
        is(RelDistributions.SINGLETON));
  }

  /** Applies {@link CoreRules#FILTER_PROJECT_TRANSPOSE} to
   * Filter(Project(Scan EMP)) and returns the distribution of the pushed
   * Filter.
   *
   * <p>The planner behind {@link RelBuilderTest#config()} does not register
   * {@link RelDistributionTraitDef}, so distributions are added through
   * {@link RelTraitSet#plus}: the Filter has {@code hash[0]}, and the scan has
   * {@code inputDistribution}, or no distribution trait when it is null. */
  private static @Nullable RelDistribution pushedFilterDistribution(
      @Nullable RelDistribution inputDistribution) {
    final RelBuilder b = RelBuilder.create(RelBuilderTest.config().build());
    final TableScan scan = (TableScan) b.scan("EMP").build();
    final RelNode input = inputDistribution == null
        ? scan
        : new LogicalTableScan(scan.getCluster(),
            scan.getTraitSet().plus(inputDistribution), scan.getHints(),
            scan.getTable());
    final Filter filter = (Filter) b.push(input)
        .project(b.field("SAL"))
        .filter(b.equals(b.field(0), b.literal(11500)))
        .build();
    final RelNode in =
        filter.copy(
            filter.getTraitSet().plus(RelDistributions.hash(ImmutableList.of(0))),
            filter.getInput(), filter.getCondition());
    final HepProgram program = new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(in);
    final RelNode result = hepPlanner.findBestExp();

    final RelNode pushedFilter = result.getInput(0);
    assertThat(pushedFilter, instanceOf(Filter.class));
    return pushedFilter.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
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

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
