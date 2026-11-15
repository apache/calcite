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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.util.Arrays;

import static org.apache.calcite.test.Matchers.sortsAs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link org.apache.calcite.rel.metadata.RelMdPredicates} class. */
public class RelMdPredicatesTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7916">[CALCITE-7916]
   * RelMdPredicates should preserve input predicates through Window</a>. */
  @Test void testPullUpConstantThroughWindow() {
    final Window window = window("select *, row_number() over (order by empno) as rn\n"
        + "from emp where deptno = 10");
    final RelMetadataQuery mq = window.getCluster().getMetadataQuery();
    final RelOptPredicateList predicates = windowPredicates(window);
    assertThat(predicates.pulledUpPredicates, sortsAs("[=($7, 10)]"));
    assertEquals(mq.getPulledUpPredicates(window.getInput()).constantMap,
        predicates.constantMap);
    assertEquals(1, predicates.constantMap.size());
    assertTrue(predicates.constantMap.keySet().stream()
        .allMatch(ref -> ref instanceof RexInputRef
            && ((RexInputRef) ref).getIndex()
                < window.getInput().getRowType().getFieldCount()));
  }

  @Test void testPullUpPredicatesThroughWindow() {
    final Window window = window("select *, row_number() over (order by empno) as rn\n"
        + "from emp where sal > 1000 and mgr is not null");
    final RelOptPredicateList predicates = windowPredicates(window);
    assertThat(predicates.pulledUpPredicates,
        sortsAs("[>($5, 1000), IS NOT NULL($3)]"));
    assertTrue(predicates.constantMap.isEmpty());
  }

  @Test void testPullUpNullConstantThroughWindow() {
    final Window window = window("select *, row_number() over (order by empno) as rn\n"
        + "from emp where mgr is null");
    final RelMetadataQuery mq = window.getCluster().getMetadataQuery();
    final RelOptPredicateList predicates = windowPredicates(window);
    assertThat(predicates.pulledUpPredicates, sortsAs("[IS NULL($3)]"));
    assertEquals(mq.getPulledUpPredicates(window.getInput()).constantMap,
        predicates.constantMap);
    assertEquals(1, predicates.constantMap.size());
  }

  @Test void testWindowWithoutInputPredicates() {
    final Window window = window("select *, row_number() over (order by empno) as rn\n"
        + "from emp");
    assertTrue(RelOptPredicateList.isEmpty(windowPredicates(window)));
  }

  @Test void testWindowConstantsAreNotOutputConstants() {
    final Window window = window("select *,\n"
        + "sum(sal) over (order by empno rows between 1 preceding and current row) as s\n"
        + "from emp where deptno = 10");
    assertEquals(1, window.constants.size());
    final RelMetadataQuery mq = window.getCluster().getMetadataQuery();
    final RelOptPredicateList predicates = windowPredicates(window);
    assertThat(predicates.pulledUpPredicates, sortsAs("[=($7, 10)]"));
    assertEquals(mq.getPulledUpPredicates(window.getInput()).constantMap,
        predicates.constantMap);
    assertEquals(1, predicates.constantMap.size());
  }

  @Test void testWindowDoesNotPullUpJoinInferredPredicates() {
    final Window window = window("select e.*, d.name,\n"
        + "row_number() over (order by e.empno) as rn\n"
        + "from (select * from emp where deptno > 10) e\n"
        + "join (select * from dept where deptno < 30) d\n"
        + "on e.deptno = d.deptno");
    final RelMetadataQuery mq = window.getCluster().getMetadataQuery();
    final RelOptPredicateList inputPredicates =
        mq.getPulledUpPredicates(window.getInput());
    assertFalse(inputPredicates.leftInferredPredicates.isEmpty());
    assertFalse(inputPredicates.rightInferredPredicates.isEmpty());
    final RelOptPredicateList predicates = windowPredicates(window);
    assertEquals(inputPredicates.pulledUpPredicates, predicates.pulledUpPredicates);
    assertEquals(inputPredicates.constantMap, predicates.constantMap);
    assertTrue(predicates.leftInferredPredicates.isEmpty());
    assertTrue(predicates.rightInferredPredicates.isEmpty());
  }

  @Test void testReduceProjectAboveWindow() {
    final RelNode rel = lowerWindows("select\n"
        + "case when deptno = 10 then rn else -1 end as selected_rn\n"
        + "from (select deptno, row_number() over (order by empno) as rn\n"
        + "from emp where deptno = 10) ranked");
    final RelNode reduced =
        applyRules(rel, CoreRules.PROJECT_REDUCE_EXPRESSIONS, CoreRules.PROJECT_MERGE);
    final Project project = assertInstanceOf(Project.class, reduced);
    final Window window = assertInstanceOf(Window.class, project.getInput());
    final RexInputRef result =
        assertInstanceOf(RexInputRef.class, project.getProjects().get(0));
    assertEquals(window.getInput().getRowType().getFieldCount(), result.getIndex());
    assertEquals(1, countFilters(window));
  }

  @Test void testReduceFilterAboveWindow() {
    final RelNode rel = lowerWindows("select *\n"
        + "from (select *, row_number() over (order by empno) as rn\n"
        + "from emp where deptno = 10) ranked\n"
        + "where deptno = 10");
    assertEquals(2, countFilters(rel));
    final RelNode reduced = applyRules(rel, CoreRules.FILTER_REDUCE_EXPRESSIONS);
    assertEquals(1, countFilters(reduced));
    final Window window = findWindow(reduced);
    assertInstanceOf(Filter.class, window.getInput());
  }

  private static Window window(String sql) {
    return findWindow(lowerWindows(sql));
  }

  private static RelOptPredicateList windowPredicates(Window window) {
    return window.getCluster().getMetadataQuery().getPulledUpPredicates(window);
  }

  private static Window findWindow(RelNode rel) {
    while (!(rel instanceof Window)) {
      assertEquals(1, rel.getInputs().size());
      rel = rel.getInput(0);
    }
    return (Window) rel;
  }

  private static RelNode lowerWindows(String sql) {
    final RelNode rel = RelMetadataFixture.DEFAULT.withSql(sql).toRel();
    return applyRules(rel, CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
  }

  private static RelNode applyRules(RelNode rel, RelOptRule... rules) {
    final HepPlanner planner = new HepPlanner(new HepProgramBuilder()
        .addRuleCollection(Arrays.asList(rules)).build());
    planner.setRoot(rel);
    return planner.findBestExp();
  }

  private static int countFilters(RelNode rel) {
    return (rel instanceof Filter ? 1 : 0)
        + rel.getInputs().stream().mapToInt(RelMdPredicatesTest::countFilters).sum();
  }

  @ParameterizedTest(name = "{0}")
  @CsvFileSource(resources = "RelMdPredicatesTestPullUpFromJoin.csv", delimiter = ';')
  void testPullUpPredicatesFromJoin(JoinRelType joinType, String expectedPredicates) {
    FrameworkConfig config = RelBuilderTest.config().build();
    RelBuilder b = RelBuilder.create(config);
    RelNode rel = b
        .scan("EMP")
        .filter(b.equals(b.field("ENAME"), b.literal("Victor")))
        .scan("DEPT")
        .filter(b.equals(b.field("DNAME"), b.literal("CSD")))
        .join(
            joinType, b.equals(
            b.field(2, 0, "DEPTNO"),
            b.field(2, 1, "DEPTNO")))
        .build();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    assertThat(list.pulledUpPredicates, sortsAs(expectedPredicates));
  }

  @ParameterizedTest(name = "{0}")
  @CsvFileSource(resources = "RelMdPredicatesTestLeftInferredFromJoin.csv", delimiter = ';')
  void testLeftInferredPredicatesFromJoin(JoinRelType joinType, String expectedPredicates) {
    FrameworkConfig config = RelBuilderTest.config().build();
    RelBuilder b = RelBuilder.create(config);
    RelNode rel = b
        .scan("EMP")
        .scan("DEPT")
        .filter(b.greaterThan(b.field("DEPTNO"), b.literal(10)))
        .join(
            joinType, b.equals(
            b.field(2, 0, "DEPTNO"),
            b.field(2, 1, "DEPTNO")))
        .build();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    assertThat(list.leftInferredPredicates, sortsAs(expectedPredicates));
  }

  @ParameterizedTest(name = "{0}")
  @CsvFileSource(resources = "RelMdPredicatesTestRightInferredFromJoin.csv", delimiter = ';')
  void testRightInferredPredicatesFromJoin(JoinRelType joinType, String expectedPredicates) {
    FrameworkConfig config = RelBuilderTest.config().build();
    RelBuilder b = RelBuilder.create(config);
    RelNode rel = b
        .scan("EMP")
        .filter(b.greaterThan(b.field("DEPTNO"), b.literal(10)))
        .scan("DEPT")
        .join(
            joinType, b.equals(
            b.field(2, 0, "DEPTNO"),
            b.field(2, 1, "DEPTNO")))
        .build();
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RelOptPredicateList list = mq.getPulledUpPredicates(rel);
    assertThat(list.rightInferredPredicates, sortsAs(expectedPredicates));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6507">[CALCITE-6507]
   * Random functions are incorrectly considered deterministic</a>. */
  @Test void testRandomFunctionsAreNotConsideredConstant() {
    FrameworkConfig config = RelBuilderTest.config().build();
    for (SqlOperator randomOp : Arrays.asList(SqlStdOperatorTable.RAND,
        SqlLibraryOperators.RANDOM, SqlStdOperatorTable.RAND_INTEGER)) {
      RelBuilder b = RelBuilder.create(config);
      RelNode rel = b
          .scan("EMP")
          .project(b.field(0), b.call(randomOp))
          .sort(1)
          .build();
      RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
      RelOptPredicateList list = mq.getPulledUpPredicates(rel);
      assertTrue(list.constantMap.isEmpty(),
          "Operator " + randomOp + " considered constant: " + list.constantMap);
    }
  }

}
