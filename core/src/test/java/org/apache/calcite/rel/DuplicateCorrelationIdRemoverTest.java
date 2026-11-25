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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalConditionalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.calcite.rel.CorrelateInvariantChecker.violations;
import static org.apache.calcite.rel.DuplicateCorrelationIdRemover.makeIdsUnique;
import static org.apache.calcite.rel.DuplicateCorrelationIdRemover.renameFreeIds;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Unit tests for {@link DuplicateCorrelationIdRemover}.
 *
 * <p>Each test builds a plan in which two declarations have the same id, and
 * compares the input plan with the output plan as a diff (see
 * {@link #renameDiff}): a line that the remover changes starts with "-" in
 * the input plan, and with "+" in the output plan.
 *
 * <p>The tests use the tables DEPT(DEPTNO, DNAME, LOC),
 * EMP(EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO) and
 * BONUS(ENAME, JOB, SAL, COMM).
 */
class DuplicateCorrelationIdRemoverTest {
  private static RelBuilder builder() {
    return RelBuilder.create(RelBuilderTest.config().build());
  }

  private static RelDataType tableRowType(RelBuilder b, String table) {
    return b.scan(table).build().getRowType();
  }

  /** Creates a use of a variable. The use has the type of a DEPT row. */
  private static RexCorrelVariable deptVariable(RelBuilder b, CorrelationId id) {
    return (RexCorrelVariable)
        b.getRexBuilder().makeCorrel(tableRowType(b, "DEPT"), id);
  }

  /** Checks that a plan obeys all the invariants. */
  private static void noViolations(RelNode plan) {
    assertThat(violations(plan), empty());
  }

  /** Returns the input plan and the output plan of the remover as one "diff" string. */
  private static String renameDiff(RelNode input, RelNode output) {
    final String[] inputLines = Util.toLinux(RelOptUtil.toString(input)).split("\n");
    final String[] outputLines = Util.toLinux(RelOptUtil.toString(output)).split("\n");
    assertThat("the remover changed the shape of the plan",
        outputLines, arrayWithSize(inputLines.length));
    final StringBuilder diff = new StringBuilder();
    for (int i = 0; i < inputLines.length; i++) {
      if (inputLines[i].equals(outputLines[i])) {
        diff.append("  ").append(inputLines[i]).append('\n');
      } else {
        diff.append("- ").append(inputLines[i]).append('\n')
            .append("+ ").append(outputLines[i]).append('\n');
      }
    }
    return diff.toString();
  }

  /** Returns the employees of the department that {@code v} refers to, with
   * a salary above {@code minSal}:
   * {@code SELECT * FROM emp WHERE deptno = v.deptno AND sal > minSal}. */
  private static RelNode empsOfDept(RelBuilder b, RexCorrelVariable v, int minSal) {
    return b.scan("EMP")
        .filter(b.equals(b.field("DEPTNO"), b.field(v, "DEPTNO")),
            b.greaterThan(b.field("SAL"), b.literal(minSal)))
        .build();
  }

  /** Returns a Correlate that declares {@code v}: for each row of DEPT, the
   * employees of that department with a salary above {@code minSal}. For
   * {@code v} = $cor0 and {@code minSal} = 1000:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])
   *   LogicalTableScan(table=[[scott, DEPT]])
   *   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), &gt;($5, 1000))])
   *     LogicalTableScan(table=[[scott, EMP]])
   * </pre></blockquote> */
  private static RelNode deptWithEmps(RelBuilder b, RexCorrelVariable v, int minSal) {
    final RelNode emps = empsOfDept(b, v, minSal);
    b.scan("DEPT").push(emps);
    return b.correlate(JoinRelType.INNER, v.id, b.field(2, 0, "DEPTNO")).build();
  }

  /** Returns a Correlate that declares {@code outer}, and has as its left
   * input a Correlate that declares {@code inner}. The two scopes do not
   * overlap. This is the plan of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5390">[CALCITE-5390]
   * RelDecorrelator throws NullPointerException</a>. For {@code inner} =
   * $cor0 and {@code outer} = $cor1:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])
   *   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])
   *     LogicalTableScan(table=[[scott, DEPT]])
   *     LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), &gt;($5, 1000))])
   *       LogicalTableScan(table=[[scott, EMP]])
   *   LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), &gt;($5, 2000))])
   *     LogicalTableScan(table=[[scott, EMP]])
   * </pre></blockquote> */
  private static RelNode correlateInLeftInput(RelBuilder b,
      RexCorrelVariable inner, RexCorrelVariable outer) {
    final RelNode left = deptWithEmps(b, inner, 1000);
    final RelNode right = empsOfDept(b, outer, 2000);
    b.push(left).push(right);
    return b.correlate(JoinRelType.INNER, outer.id, b.field(2, 0, "DEPTNO")).build();
  }

  /** Returns a Correlate that declares {@code outer}, and has in its right
   * input a Correlate that declares {@code inner}. A Filter between the two
   * Correlates uses {@code outer}. This is the plan of
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7753">[CALCITE-7753]
   * CorrelateProjectExtractor corrupts plans with nested Correlates that reuse
   * the same correlation id</a>. For {@code inner} = $cor0 and
   * {@code outer} = $cor1:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
   *   LogicalTableScan(table=[[scott, DEPT]])
   *   LogicalFilter(condition=[=($1, $cor1.DNAME)])
   *     LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])
   *       LogicalTableScan(table=[[scott, DEPT]])
   *       LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), &gt;($5, 1000))])
   *         LogicalTableScan(table=[[scott, EMP]])
   * </pre></blockquote> */
  private static RelNode correlateInRightInput(RelBuilder b,
      RexCorrelVariable inner, RexCorrelVariable outer) {
    final RelNode right = b.push(deptWithEmps(b, inner, 1000))
        .filter(b.equals(b.field("DNAME"), b.field(outer, "DNAME")))
        .build();
    b.scan("DEPT").push(right);
    return b.correlate(JoinRelType.INNER, outer.id, b.field(2, 0, "DNAME")).build();
  }

  /** Returns a Correlate that declares {@code outer}. Its right input is a
   * Filter that declares {@code filterVariable}. The condition of the Filter
   * uses {@code outer}, and its sub-query uses {@code filterVariable}. For
   * {@code filterVariable} = $cor0 and {@code outer} = $cor1:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
   *   LogicalTableScan(table=[[scott, DEPT]])
   *   LogicalFilter(condition=[AND(=($1, $cor1.DNAME), EXISTS({
   * LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), &gt;($5, 1000))])
   *   LogicalTableScan(table=[[scott, EMP]])
   * }))], variablesSet=[[$cor0]])
   *     LogicalTableScan(table=[[scott, DEPT]])
   * </pre></blockquote> */
  private static RelNode filterInCorrelate(RelBuilder b,
      RexCorrelVariable filterVariable, RexCorrelVariable outer) {
    final RelNode right = b.scan("DEPT")
        .filter(ImmutableList.of(filterVariable.id),
            b.equals(b.field("DNAME"), b.field(outer, "DNAME")),
            b.exists(b2 -> empsOfDept(b2, filterVariable, 1000)))
        .build();
    b.scan("DEPT").push(right);
    return b.correlate(JoinRelType.INNER, outer.id, b.field(2, 0, "DNAME")).build();
  }

  /** Returns a Correlate that declares {@code outer}. Its right input is a
   * Project that declares {@code projectVariable}. One expression of the
   * Project uses {@code outer}, and its scalar sub-query uses
   * {@code projectVariable}. For {@code projectVariable} = $cor0 and
   * {@code outer} = $cor1:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
   *   LogicalTableScan(table=[[scott, DEPT]])
   *   LogicalProject(variablesSet=[[$cor0]], D=[$cor1.DNAME], C=[$SCALAR_QUERY({
   * LogicalAggregate(group=[{}], C=[COUNT()])
   *   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), &gt;($5, 1000))])
   *     LogicalTableScan(table=[[scott, EMP]])
   * })])
   *     LogicalTableScan(table=[[scott, DEPT]])
   * </pre></blockquote> */
  private static RelNode projectInCorrelate(RelBuilder b,
      RexCorrelVariable projectVariable, RexCorrelVariable outer) {
    final RelNode right = b.scan("DEPT")
        .project(
            ImmutableList.of(b.field(outer, "DNAME"),
                b.scalarQuery(b2 ->
                    b2.push(empsOfDept(b2, projectVariable, 1000))
                        .aggregate(b2.groupKey(), b2.countStar("C"))
                        .build())),
            ImmutableList.of("D", "C"), false, ImmutableList.of(projectVariable.id))
        .build();
    b.scan("DEPT").push(right);
    return b.correlate(JoinRelType.INNER, outer.id, b.field(2, 0, "DNAME")).build();
  }

  /** Returns a Correlate that declares {@code outer}. Its right input is a
   * Join that declares {@code joinVariable}. The ON condition uses
   * {@code outer}, and its sub-query uses {@code joinVariable}. For
   * {@code joinVariable} = $cor0 and {@code outer} = $cor1:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
   *   LogicalTableScan(table=[[scott, DEPT]])
   *   LogicalJoin(condition=[AND(EXISTS({
   * LogicalFilter(condition=[=($0, $cor0.ENAME)])
   *   LogicalTableScan(table=[[scott, BONUS]])
   * }), =($9, $cor1.DNAME))], joinType=[inner], variablesSet=[[$cor0]])
   *     LogicalTableScan(table=[[scott, EMP]])
   *     LogicalTableScan(table=[[scott, DEPT]])
   * </pre></blockquote> */
  private static RelNode joinInCorrelate(RelBuilder b,
      RexCorrelVariable joinVariable, RexCorrelVariable outer) {
    final RelNode right = b.scan("EMP").scan("DEPT")
        .join(JoinRelType.INNER,
            b.and(
                b.exists(b2 ->
                    b2.scan("BONUS")
                        .filter(b2.equals(b2.field("ENAME"), b2.field(joinVariable, "ENAME")))
                        .build()),
                b.equals(b.field(2, 1, "DNAME"), b.field(outer, "DNAME"))),
            ImmutableSet.of(joinVariable.id))
        .build();
    b.scan("DEPT").push(right);
    return b.correlate(JoinRelType.INNER, outer.id, b.field(2, 0, "DNAME")).build();
  }

  /** Returns a Correlate that declares {@code outer}. Its right input is a
   * Filter that declares {@code filterVariable}. The sub-query of the Filter
   * is a Correlate that declares {@code empVariable}, and the left input of
   * that Correlate uses {@code filterVariable}. For {@code filterVariable} =
   * $cor0, {@code outer} = $cor1 and {@code empVariable} = $cor2:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
   *   LogicalTableScan(table=[[scott, DEPT]])
   *   LogicalFilter(condition=[AND(=($1, $cor1.DNAME), EXISTS({
   * LogicalCorrelate(correlation=[$cor2], joinType=[inner], requiredColumns=[{7}])
   *   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), &gt;($5, 1000))])
   *     LogicalTableScan(table=[[scott, EMP]])
   *   LogicalFilter(condition=[=($0, $cor2.DEPTNO)])
   *     LogicalTableScan(table=[[scott, DEPT]])
   * }))], variablesSet=[[$cor0]])
   *     LogicalTableScan(table=[[scott, DEPT]])
   * </pre></blockquote> */
  private static RelNode correlateInSubQuery(RelBuilder b,
      RexCorrelVariable filterVariable, RexCorrelVariable outer,
      RexCorrelVariable empVariable) {
    final RelNode right = b.scan("DEPT")
        .filter(ImmutableList.of(filterVariable.id),
            b.equals(b.field("DNAME"), b.field(outer, "DNAME")),
            b.exists(b2 -> {
              final RelNode left = empsOfDept(b2, filterVariable, 1000);
              final RelNode deptOfEmp = b2.scan("DEPT")
                  .filter(b2.equals(b2.field("DEPTNO"), b2.field(empVariable, "DEPTNO")))
                  .build();
              return b2.push(left).push(deptOfEmp)
                  .correlate(JoinRelType.INNER, empVariable.id, b2.field(2, 0, "DEPTNO"))
                  .build();
            }))
        .build();
    b.scan("DEPT").push(right);
    return b.correlate(JoinRelType.INNER, outer.id, b.field(2, 0, "DNAME")).build();
  }

  /** Returns a Correlate that declares {@code outer}, and has as its left
   * input a mark Correlate (a {@link LogicalConditionalCorrelate}) that
   * declares {@code inner}. The condition of the mark Correlate compares the
   * DEPTNO of its two inputs. For {@code inner} = $cor0 and {@code outer} =
   * $cor1 (the second line is wrapped):
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])
   *   LogicalConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], requiredColumns=[{0}],
   *       condition=[=($0, $10)])
   *     LogicalTableScan(table=[[scott, DEPT]])
   *     LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), &gt;($5, 1000))])
   *       LogicalTableScan(table=[[scott, EMP]])
   *   LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), &gt;($5, 2000))])
   *     LogicalTableScan(table=[[scott, EMP]])
   * </pre></blockquote> */
  private static RelNode markCorrelateInLeftInput(RelBuilder b,
      RexCorrelVariable inner, RexCorrelVariable outer) {
    b.scan("DEPT").push(empsOfDept(b, inner, 1000));
    final RexNode condition = b.equals(b.field(2, 0, "DEPTNO"), b.field(2, 1, "DEPTNO"));
    final RelNode emps = b.build();
    final RelNode dept = b.build();
    final RelNode mark =
        LogicalConditionalCorrelate.create(dept, emps, ImmutableList.of(), inner.id,
            ImmutableBitSet.of(0), JoinRelType.LEFT_MARK, condition);
    b.push(mark).push(empsOfDept(b, outer, 2000));
    return b.correlate(JoinRelType.INNER, outer.id, b.field(2, 0, "DEPTNO")).build();
  }

  /** Returns a Correlate that declares {@code outer}. Its right input is a
   * Filter that declares {@code filterVariable}. The sub-query of the Filter
   * is a mark Correlate, and the condition of the mark Correlate uses
   * {@code filterVariable}. For {@code filterVariable} = $cor0 and
   * {@code outer} = $cor1 (the mark Correlate gets $cor2; its line is
   * wrapped):
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
   *   LogicalTableScan(table=[[scott, DEPT]])
   *   LogicalFilter(condition=[AND(=($1, $cor1.DNAME), EXISTS({
   * LogicalConditionalCorrelate(correlation=[$cor2], joinType=[left_mark], requiredColumns=[{}],
   *     condition=[AND(=($1, $8), =($7, $cor0.DEPTNO))])
   *   LogicalTableScan(table=[[scott, EMP]])
   *   LogicalTableScan(table=[[scott, BONUS]])
   * }))], variablesSet=[[$cor0]])
   *     LogicalTableScan(table=[[scott, DEPT]])
   * </pre></blockquote> */
  private static RelNode markCorrelateInSubQuery(RelBuilder b,
      RexCorrelVariable filterVariable, RexCorrelVariable outer) {
    final RelNode right = b.scan("DEPT")
        .filter(ImmutableList.of(filterVariable.id),
            b.equals(b.field("DNAME"), b.field(outer, "DNAME")),
            b.exists(b2 -> {
              b2.scan("EMP").scan("BONUS");
              final RexNode condition =
                  b2.and(b2.equals(b2.field(2, 0, "ENAME"), b2.field(2, 1, "ENAME")),
                      b2.equals(b2.field(2, 0, "DEPTNO"),
                          b2.field(filterVariable, "DEPTNO")));
              final RelNode bonus = b2.build();
              final RelNode emp = b2.build();
              return LogicalConditionalCorrelate.create(emp, bonus, ImmutableList.of(),
                  b2.getCluster().createCorrel(), ImmutableBitSet.of(),
                  JoinRelType.LEFT_MARK, condition);
            }))
        .build();
    b.scan("DEPT").push(right);
    return b.correlate(JoinRelType.INNER, outer.id, b.field(2, 0, "DNAME")).build();
  }

  /** Returns a variable for the Join in {@link #joinInCorrelate}: the use has
   * the type of an EMP row followed by a DEPT row. */
  private static RexCorrelVariable empThenDeptVariable(RelBuilder b, CorrelationId id) {
    final RelDataType empThenDept =
        SqlValidatorUtil.createJoinType(b.getTypeFactory(),
            tableRowType(b, "EMP"), tableRowType(b, "DEPT"), null, ImmutableList.of());
    return (RexCorrelVariable) b.getRexBuilder().makeCorrel(empThenDept, id);
  }

  /** Returns a plan with {@code depth} Unions above {@code node}. Each Union
   * has one node as its two inputs, thus the plan is a DAG with 2^depth
   * paths from its root to {@code node}. */
  private static RelNode unionLadder(RelBuilder b, RelNode node, int depth) {
    RelNode ladder = node;
    for (int i = 0; i < depth; i++) {
      ladder = b.push(ladder).push(ladder).union(true).build();
    }
    return ladder;
  }

  /** Returns a Correlate that declares {@code declared}. Its right input uses
   * {@code declared}, and also {@code free}, which no node of the plan
   * declares. For {@code declared} = $cor1 and {@code free} = $cor0:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])
   *   LogicalTableScan(table=[[scott, DEPT]])
   *   LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), =($1, $cor0.DNAME))])
   *     LogicalTableScan(table=[[scott, EMP]])
   * </pre></blockquote> */
  private static RelNode correlateWithFreeUse(RelBuilder b,
      RexCorrelVariable declared, RexCorrelVariable free) {
    final RelNode emps = b.scan("EMP")
        .filter(b.equals(b.field("DEPTNO"), b.field(declared, "DEPTNO")),
            b.equals(b.field("ENAME"), b.field(free, "DNAME")))
        .build();
    b.scan("DEPT").push(emps);
    return b.correlate(JoinRelType.INNER, declared.id, b.field(2, 0, "DEPTNO")).build();
  }

  @Test void testPlanWithUniqueIdsIsNotChanged() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RexCorrelVariable w = deptVariable(b, b.getCluster().createCorrel());
    final RelNode correlates = correlateInLeftInput(b, w, v);
    noViolations(correlates);
    assertThat(makeIdsUnique(correlates), sameInstance(correlates));

    final RelNode filter = filterInCorrelate(b, w, v);
    noViolations(filter);
    assertThat(makeIdsUnique(filter), sameInstance(filter));
  }

  /** The inner Correlate is in the left input of the outer Correlate, and
   * the two declare the same id. The outer Correlate keeps the id. The inner
   * Correlate, and the uses in its right input, get a new id. */
  @Test void testCorrelateInLeftInputWithSameId() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RelNode plan = correlateInLeftInput(b, v, v);
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "-   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "+   LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-     LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+     LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 2000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    noViolations(renamed);
  }

  /** The inner Correlate is in the right input of the outer Correlate, and
   * the two declare the same id. The use in the Filter between them refers
   * to the outer Correlate, and keeps the id. */
  @Test void testCorrelateInRightInputWithSameId() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RelNode plan = correlateInRightInput(b, v, v);
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[=($1, $cor0.DNAME)])\n"
        + "-     LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "+     LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-       LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+       LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), >($5, 1000))])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    noViolations(renamed);
  }

  /** A Filter in the right input of a Correlate declares the id of the
   * Correlate. The use in the condition of the Filter refers to the
   * Correlate, and keeps the id. The use in the sub-query refers to the
   * Filter, and gets a new id. */
  @Test void testFilterWithSameIdAsCorrelate() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RelNode plan = filterInCorrelate(b, v, v);
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[AND(=($1, $cor0.DNAME), EXISTS({\n"
        + "- LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+ LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), >($5, 1000))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "- }))], variablesSet=[[$cor0]])\n"
        + "+ }))], variablesSet=[[$cor1]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    noViolations(renamed);
  }

  /** As {@link #testFilterWithSameIdAsCorrelate()}, but the use in the
   * sub-query is in the left input of a Correlate. The variable of that
   * Correlate is $cor1, and keeps its id. */
  @Test void testUseInLeftInputOfCorrelateGetsNewId() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RexCorrelVariable empVariable = (RexCorrelVariable)
        b.getRexBuilder().makeCorrel(tableRowType(b, "EMP"), b.getCluster().createCorrel());
    final RelNode plan = correlateInSubQuery(b, v, v, empVariable);
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[AND(=($1, $cor0.DNAME), EXISTS({\n"
        + "  LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{7}])\n"
        + "-   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+   LogicalFilter(condition=[AND(=($7, $cor2.DEPTNO), >($5, 1000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[=($0, $cor1.DEPTNO)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "- }))], variablesSet=[[$cor0]])\n"
        + "+ }))], variablesSet=[[$cor2]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    noViolations(renamed);
  }

  /** As {@link #testCorrelateInLeftInputWithSameId()}, but the inner
   * Correlate is a mark Correlate, which has a condition. */
  @Test void testMarkCorrelateWithSameId() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RelNode plan = markCorrelateInLeftInput(b, v, v);
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "-   LogicalConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], "
        + "requiredColumns=[{0}], condition=[=($0, $10)])\n"
        + "+   LogicalConditionalCorrelate(correlation=[$cor1], joinType=[left_mark], "
        + "requiredColumns=[{0}], condition=[=($0, $10)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-     LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+     LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 2000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    noViolations(renamed);
  }

  /** The condition of a mark Correlate uses the variable of a Filter, and
   * the variable gets a new id. The use in the condition gets the new id
   * too. */
  @Test void testUseInConditionOfMarkCorrelateGetsNewId() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RelNode plan = markCorrelateInSubQuery(b, v, v);
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[AND(=($1, $cor0.DNAME), EXISTS({\n"
        + "- LogicalConditionalCorrelate(correlation=[$cor1], joinType=[left_mark], "
        + "requiredColumns=[{}], condition=[AND(=($1, $8), =($7, $cor0.DEPTNO))])\n"
        + "+ LogicalConditionalCorrelate(correlation=[$cor1], joinType=[left_mark], "
        + "requiredColumns=[{}], condition=[AND(=($1, $8), =($7, $cor2.DEPTNO))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalTableScan(table=[[scott, BONUS]])\n"
        + "- }))], variablesSet=[[$cor0]])\n"
        + "+ }))], variablesSet=[[$cor2]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    noViolations(renamed);
  }

  /** As {@link #testFilterWithSameIdAsCorrelate()}, for a Project. */
  @Test void testProjectWithSameIdAsCorrelate() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RelNode plan = projectInCorrelate(b, v, v);
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-   LogicalProject(variablesSet=[[$cor0]], D=[$cor0.DNAME], C=[$SCALAR_QUERY({\n"
        + "+   LogicalProject(variablesSet=[[$cor1]], D=[$cor0.DNAME], C=[$SCALAR_QUERY({\n"
        + "  LogicalAggregate(group=[{}], C=[COUNT()])\n"
        + "-   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+   LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), >($5, 1000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "  })])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    noViolations(renamed);
  }

  /** As {@link #testFilterWithSameIdAsCorrelate()}, for a Join. */
  @Test void testJoinWithSameIdAsCorrelate() {
    final RelBuilder b = builder();
    final CorrelationId id = b.getCluster().createCorrel();
    final RelNode plan =
        joinInCorrelate(b, empThenDeptVariable(b, id), deptVariable(b, id));
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalJoin(condition=[AND(EXISTS({\n"
        + "- LogicalFilter(condition=[=($0, $cor0.ENAME)])\n"
        + "+ LogicalFilter(condition=[=($0, $cor1.ENAME)])\n"
        + "    LogicalTableScan(table=[[scott, BONUS]])\n"
        + "- }), =($9, $cor0.DNAME))], joinType=[inner], variablesSet=[[$cor0]])\n"
        + "+ }), =($9, $cor0.DNAME))], joinType=[inner], variablesSet=[[$cor1]])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    noViolations(renamed);
  }

  /** The plan uses $cor2, the id that the cluster gives next, and no node
   * declares $cor2. The new id is $cor3, and the free variable keeps $cor2. */
  @Test void testNewIdIsNotAnIdOfThePlan() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final CorrelationId next =
        new CorrelationId(b.getCluster().createCorrel().getId() + 1);
    final RelNode plan = b.push(correlateInLeftInput(b, v, v))
        .filter(b.equals(b.field("DEPTNO"), b.field(deptVariable(b, next), "DEPTNO")))
        .build();
    final RelNode renamed = makeIdsUnique(plan);
    final String diff = ""
        + "  LogicalFilter(condition=[=($0, $cor2.DEPTNO)])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "-     LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "+     LogicalCorrelate(correlation=[$cor3], joinType=[inner], requiredColumns=[{0}])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-       LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+       LogicalFilter(condition=[AND(=($7, $cor3.DEPTNO), >($5, 1000))])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 2000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    // The only violation is the use of the free variable
    assertThat(violations(renamed),
        contains(allOf(startsWith(next + " is used in"), endsWith("but no node declares it"))));
  }

  /** The plan is a DAG with 2^40 paths from its root to two Correlates that
   * declare the same id. The remover visits each node one time for each
   * renaming of its free ids, and the output plan is a DAG too. */
  @Test void testDagWithManyPaths() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = deptVariable(b, b.getCluster().createCorrel());
    final RelNode correlates = correlateInLeftInput(b, v, v);

    // A plan with 1 Union. The two inputs of the Union are one node. The
    // output gives one new id to that node, and keeps one node.
    final RelNode oneUnion = unionLadder(b, correlates, 1);
    final RelNode renamedUnion = makeIdsUnique(oneUnion);
    final String diff = ""
        + "  LogicalUnion(all=[true])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "-     LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "+     LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-       LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+       LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), >($5, 1000))])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 2000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "-     LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "+     LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-       LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "+       LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), >($5, 1000))])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 2000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(renameDiff(oneUnion, renamedUnion), is(diff));
    assertThat(renamedUnion.getInput(1), sameInstance(renamedUnion.getInput(0)));

    // The same plan with 40 Unions
    final RelNode plan = unionLadder(b, correlates, 40);
    final RelNode renamed =
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> makeIdsUnique(plan));
    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> noViolations(renamed));
  }

  /** {@link DuplicateCorrelationIdRemover#renameFreeIds} gives the new id to
   * the free variable, and not to the variable of the Correlate. */
  @Test void testRenameFreeIds() {
    final RelBuilder b = builder();
    final RexCorrelVariable free = deptVariable(b, b.getCluster().createCorrel());
    final RexCorrelVariable declared = deptVariable(b, b.getCluster().createCorrel());
    final RelNode plan = correlateWithFreeUse(b, declared, free);
    final CorrelationId newId = b.getCluster().createCorrel();
    final RelNode renamed = renameFreeIds(plan, ImmutableMap.of(free.id, newId));
    final String diff = ""
        + "  LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-   LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), =($1, $cor0.DNAME))])\n"
        + "+   LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), =($1, $cor2.DNAME))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
  }

  /** The new id of the free variable is the id that the Correlate declares.
   * The Correlate gets another id, so that it does not capture the use: the
   * variable stays free. */
  @Test void testRenameFreeIdsAvoidsCapture() {
    final RelBuilder b = builder();
    final RexCorrelVariable free = deptVariable(b, b.getCluster().createCorrel());
    final RexCorrelVariable declared = deptVariable(b, b.getCluster().createCorrel());
    final RelNode plan = correlateWithFreeUse(b, declared, free);
    final RelNode renamed = renameFreeIds(plan, ImmutableMap.of(free.id, declared.id));
    final String diff = ""
        + "- LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{0}])\n"
        + "+ LogicalCorrelate(correlation=[$cor2], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "-   LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), =($1, $cor0.DNAME))])\n"
        + "+   LogicalFilter(condition=[AND(=($7, $cor2.DEPTNO), =($1, $cor1.DNAME))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(renameDiff(plan, renamed), is(diff));
    assertThat(RelOptUtil.getVariablesUsed(renamed), is(ImmutableSet.of(declared.id)));
  }
}
