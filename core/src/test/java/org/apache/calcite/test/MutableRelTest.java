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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.mutable.MutableRel;
import org.apache.calcite.rel.mutable.MutableRels;
import org.apache.calcite.rel.mutable.MutableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.plan.RelOptUtil.equal;
import static org.apache.calcite.util.Litmus.IGNORE;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MutableRel} sub-classes.
 */
class MutableRelTest {

  @Test void testConvertAggregate() {
    checkConvertMutableRel(
        "Aggregate",
        "select empno, sum(sal) from emp group by empno");
  }

  @Test void testConvertFilter() {
    checkConvertMutableRel(
        "Filter",
        "select * from emp where ename = 'DUMMY'");
  }

  @Test void testConvertProject() {
    checkConvertMutableRel(
        "Project",
        "select ename from emp");
  }

  @Test void testConvertSort() {
    checkConvertMutableRel(
        "Sort",
        "select * from emp order by ename");
  }

  @Test void testConvertCalc() {
    checkConvertMutableRel(
        "Calc",
        "select * from emp where ename = 'DUMMY'",
        false,
        ImmutableList.of(CoreRules.FILTER_TO_CALC));
  }

  @Test void testConvertWindow() {
    checkConvertMutableRel(
        "Window",
        "select sal, avg(sal) over (partition by deptno) from emp",
        false,
        ImmutableList.of(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW));
  }

  @Test void testConvertCollect() {
    checkConvertMutableRel(
        "Collect",
        "select multiset(select deptno from dept) from (values(true))");
  }

  @Test void testConvertUncollect() {
    checkConvertMutableRel(
        "Uncollect",
        "select * from unnest(multiset[1,2])");
  }

  @Test void testConvertTableModify() {
    checkConvertMutableRel(
        "TableModify",
        "insert into dept select empno, ename from emp");
  }

  @Test void testConvertSample() {
    checkConvertMutableRel(
        "Sample",
        "select * from emp tablesample system(50) where empno > 5");
  }

  @Test void testConvertTableFunctionScan() {
    checkConvertMutableRel(
        "TableFunctionScan",
        "select * from table(ramp(3))");
  }

  @Test void testConvertValues() {
    checkConvertMutableRel(
        "Values",
        "select * from (values (1, 2))");
  }

  @Test void testConvertJoin() {
    checkConvertMutableRel(
        "Join",
        "select * from emp join dept using (deptno)");
  }

  @Test void testConvertSemiJoin() {
    final String sql = "select * from dept where exists (\n"
        + "  select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.sal > 100)";
    checkConvertMutableRel(
        "Join", // with join type as semi
        sql,
        true,
        ImmutableList.of(
            CoreRules.FILTER_PROJECT_TRANSPOSE, CoreRules.FILTER_INTO_JOIN, CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_TO_SEMI_JOIN));
  }

  @Test void testConvertCorrelate() {
    final String sql = "select * from dept where exists (\n"
        + "  select * from emp\n"
        + "  where emp.deptno = dept.deptno\n"
        + "  and emp.sal > 100)";
    checkConvertMutableRel("Correlate", sql);
  }

  @Test void testConvertUnion() {
    checkConvertMutableRel(
        "Union",
        "select * from emp where deptno = 10"
        + "union select * from emp where ename like 'John%'");
  }

  @Test void testConvertMinus() {
    checkConvertMutableRel(
        "Minus",
        "select * from emp where deptno = 10"
        + "except select * from emp where ename like 'John%'");
  }

  @Test void testConvertIntersect() {
    checkConvertMutableRel(
        "Intersect",
        "select * from emp where deptno = 10"
        + "intersect select * from emp where ename like 'John%'");
  }

  @Test void testUpdateInputOfUnion() {
    MutableRel mutableRel =
        createMutableRel("select sal from emp where deptno = 10"
            + "union select sal from emp where ename like 'John%'");
    MutableRel childMutableRel =
        createMutableRel("select sal from emp where deptno = 12");
    mutableRel.setInput(0, childMutableRel);
    String actual = RelOptUtil.toString(MutableRels.fromMutable(mutableRel));
    String expected = ""
        + "LogicalUnion(all=[false])\n"
        + "  LogicalProject(SAL=[$5])\n"
        + "    LogicalFilter(condition=[=($7, 12)])\n"
        + "      LogicalTableScan(table=[[CATALOG, SALES, EMP]])\n"
        + "  LogicalProject(SAL=[$5])\n"
        + "    LogicalFilter(condition=[LIKE($1, 'John%')])\n"
        + "      LogicalTableScan(table=[[CATALOG, SALES, EMP]])\n";
    MatcherAssert.assertThat(actual, Matchers.isLinux(expected));
  }

  @Test void testParentInfoOfUnion() {
    MutableRel mutableRel =
        createMutableRel("select sal from emp where deptno = 10"
            + "union select sal from emp where ename like 'John%'");
    for (MutableRel input : mutableRel.getInputs()) {
      assertSame(input.getParent(), mutableRel);
    }
  }

  @Test void testMutableTableFunctionScanEquals() {
    final String sql = "SELECT * FROM TABLE(RAMP(3))";
    final MutableRel mutableRel1 = createMutableRel(sql);
    final MutableRel mutableRel2 = createMutableRel(sql);
    final String actual = RelOptUtil.toString(MutableRels.fromMutable(mutableRel1));
    final String expected = ""
        + "LogicalProject(I=[$0])\n"
        + "  LogicalTableFunctionScan(invocation=[RAMP(3)], rowType=[RecordType(INTEGER I)])\n";
    MatcherAssert.assertThat(actual, Matchers.isLinux(expected));
    assertThat(mutableRel2, is(mutableRel1));
  }

  /** Verifies equivalence of {@link MutableScan}. */
  @Test void testMutableScanEquivalence() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);

    assertThat(mutableScanOf(builder, "EMP"),
        equalTo(mutableScanOf(builder, "EMP")));
    assertThat(mutableScanOf(builder, "EMP").hashCode(),
        equalTo(mutableScanOf(builder, "EMP").hashCode()));

    assertThat(mutableScanOf(builder, "scott", "EMP"),
        equalTo(mutableScanOf(builder, "scott", "EMP")));
    assertThat(mutableScanOf(builder, "scott", "EMP").hashCode(),
        equalTo(mutableScanOf(builder, "scott", "EMP").hashCode()));

    assertThat(mutableScanOf(builder, "scott", "EMP"),
        equalTo(mutableScanOf(builder, "EMP")));
    assertThat(mutableScanOf(builder, "scott", "EMP").hashCode(),
        equalTo(mutableScanOf(builder, "EMP").hashCode()));

    assertThat(mutableScanOf(builder, "EMP"),
        not(equalTo(mutableScanOf(builder, "DEPT"))));
  }

  /** Verifies that after conversion to and from a MutableRel, the new
   * RelNode remains identical to the original RelNode. */
  private static void checkConvertMutableRel(String rel, String sql) {
    checkConvertMutableRel(rel, sql, false, null);
  }

  /** Verifies that after conversion to and from a MutableRel, the new
   * RelNode remains identical to the original RelNode. */
  private static void checkConvertMutableRel(
      String rel, String sql, boolean decorrelate, List<RelOptRule> rules) {
    final SqlToRelFixture fixture =
        SqlToRelFixture.DEFAULT.withSql(sql).withDecorrelate(decorrelate);
    RelNode origRel = fixture.toRel();
    if (rules != null) {
      final HepProgram hepProgram =
          new HepProgramBuilder().addRuleCollection(rules).build();
      final HepPlanner hepPlanner = new HepPlanner(hepProgram);
      hepPlanner.setRoot(origRel);
      origRel = hepPlanner.findBestExp();
    }
    // Convert to and from a mutable rel.
    final MutableRel mutableRel = MutableRels.toMutable(origRel);
    final RelNode newRel = MutableRels.fromMutable(mutableRel);

    // Check if the mutable rel digest contains the target rel.
    final String mutableRelStr = mutableRel.deep();
    final String msg1 =
        "Mutable rel: " + mutableRelStr + " does not contain target rel: " + rel;
    assertTrue(mutableRelStr.contains(rel), msg1);

    // Check if the mutable rel's row-type is identical to the original
    // rel's row-type.
    final RelDataType origRelType = origRel.getRowType();
    final RelDataType mutableRelType = mutableRel.rowType;
    final String msg2 =
        "Mutable rel's row type does not match with the original rel.\n"
        + "Original rel type: " + origRelType
        + ";\nMutable rel type: " + mutableRelType;
    assertTrue(
        equal(
            "origRelType", origRelType,
            "mutableRelType", mutableRelType,
            IGNORE),
        msg2);

    // Check if the new rel converted from the mutable rel is identical
    // to the original rel.
    final String origRelStr = RelOptUtil.toString(origRel);
    final String newRelStr = RelOptUtil.toString(newRel);
    final String msg3 =
        "The converted new rel is different from the original rel.\n"
        + "Original rel: " + origRelStr + ";\nNew rel: " + newRelStr;
    assertThat(msg3, newRelStr, is(origRelStr));
  }

  private static MutableRel createMutableRel(String sql) {
    RelNode rel = SqlToRelFixture.DEFAULT.withSql(sql).toRel();
    return MutableRels.toMutable(rel);
  }

  private MutableScan mutableScanOf(RelBuilder builder, String... tableNames) {
    final RelNode scan = builder.scan(tableNames).build();
    return (MutableScan) MutableRels.toMutable(scan);
  }
}
