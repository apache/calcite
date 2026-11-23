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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.rel.CorrelateInvariantChecker.violations;
import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import static java.lang.Integer.parseInt;

/**
 * Unit tests for {@link CorrelateInvariantChecker}.
 *
 * <p>The tests build each plan by using {@link RelBuilder}. A test with
 * a name that ends in "Valid" builds a correct plan. The other tests build
 * a plan that is intentionally incorrect: the plan breaks an invariant, and the
 * test checks that the checker reports it. Such a test shows the plan with
 * "&gt; " before each incorrect line of a plan (see {@link #markedPlan}).
 *
 * <p>The tests use the tables DEPT(DEPTNO, DNAME, LOC) and
 * EMP(EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO).
 */
class CorrelateInvariantCheckerTest {
  /** Matches the id ({@link RelNode#getId()}) of a plan node in a message,
   * for example "#38". */
  private static final Pattern NODE_ID = Pattern.compile("#(\\d+)");

  private static RelBuilder builder() {
    return RelBuilder.create(RelBuilderTest.config().build());
  }

  private static RelDataType tableRowType(RelBuilder b, String table) {
    return b.scan(table).build().getRowType();
  }

  /** Creates a variable with a new id and the given type. */
  private static RexCorrelVariable variable(RelBuilder b, RelDataType type) {
    return (RexCorrelVariable)
        b.getRexBuilder().makeCorrel(type, b.getCluster().createCorrel());
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
   * employees of that department, as in
   * {@code SELECT * FROM dept, LATERAL (SELECT * FROM emp
   * WHERE emp.deptno = dept.deptno AND emp.sal > minSal)}. */
  private static RelNode deptWithEmps(RelBuilder b, RexCorrelVariable v, int minSal) {
    final RelNode emps = empsOfDept(b, v, minSal);
    b.scan("DEPT").push(emps);
    return b.correlate(JoinRelType.INNER, v.id, b.field(2, 0, "DEPTNO"))
        .build();
  }

  /** Returns a Correlate that declares {@code v}. Its right input reads the
   * full row of {@code v}: {@code SELECT empno, v AS d FROM emp}.
   *
   * @param requiredFields Fields of DEPT in the required columns of the Correlate
   */
  private static RelNode deptWithFullRowUse(RelBuilder b, RexCorrelVariable v,
      String... requiredFields) {
    final RelNode right =
        b.scan("EMP").project(b.field("EMPNO"), b.alias(v, "D")).build();
    b.scan("DEPT").push(right);
    final ImmutableList.Builder<RexNode> required = ImmutableList.builder();
    for (String field : requiredFields) {
      required.add(b.field(2, 0, field));
    }
    return b.correlate(JoinRelType.INNER, v.id, required.build()).build();
  }

  /** Returns the type of a DEPT row with one more field at the end. */
  private static RelDataType deptRowTypeWithExtraField(RelBuilder b) {
    return b.getTypeFactory().builder()
        .addAll(tableRowType(b, "DEPT").getFieldList())
        .add("EXTRA", SqlTypeName.INTEGER)
        .build();
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

  /** Returns the messages of the checker for a plan, without the ids of the
   * rel nodes: "LogicalFilter#38" becomes "LogicalFilter". */
  private static List<String> messages(RelNode plan) {
    final ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (String message : violations(plan)) {
      builder.add(NODE_ID.matcher(message).replaceAll(""));
    }
    return builder.build();
  }

  /** Adds to {@code lines} the first line of the text of each node whose id
   * ({@link RelNode#getId()}) is in {@code ids}. Also visits the plans of
   * sub-queries.
   *
   * <p>For example, the plan of {@link #testCorrelateIsValid()}, as
   * {@code RelOptUtil.toString(plan, SqlExplainLevel.NON_COST_ATTRIBUTES)}
   * prints it, with the id of each node:
   *
   * <blockquote><pre>
   * LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}]), id = 4
   *   LogicalTableScan(table=[[scott, DEPT]]), id = 3
   *   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), &gt;($5, 1000))]), id = 2
   *     LogicalTableScan(table=[[scott, EMP]]), id = 1
   * </pre></blockquote>
   *
   * <p>If {@code ids} is {4}, this method adds the first line of the text of
   * the Correlate, without the id and without the inputs:
   * {@code LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])}. */
  private static void addLinesOfNodes(RelNode node, Set<Integer> ids,
      Set<String> lines) {
    if (ids.contains(node.getId())) {
      lines.add(Util.toLinux(RelOptUtil.toString(node)).split("\n")[0]);
    }
    for (RelNode input : node.getInputs()) {
      addLinesOfNodes(input, ids, lines);
    }
    node.accept(new RexShuttle() {
      @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
        addLinesOfNodes(subQuery.rel, ids, lines);
        return super.visitSubQuery(subQuery);
      }
    });
  }

  /** Returns the text of a plan. Each line of a rel node that a message of the
   * checker flags starts with "> ". */
  private static String markedPlan(RelNode plan) {
    final Set<Integer> ids = new HashSet<>();
    for (String message : violations(plan)) {
      final Matcher matcher = NODE_ID.matcher(message);
      while (matcher.find()) {
        ids.add(parseInt(matcher.group(1)));
      }
    }
    final Set<String> marked = new HashSet<>();
    addLinesOfNodes(plan, ids, marked);
    final StringBuilder text = new StringBuilder();
    for (String line : Util.toLinux(RelOptUtil.toString(plan)).split("\n")) {
      text.append(marked.contains(line.trim()) ? "> " : "  ").append(line).append('\n');
    }
    return text.toString();
  }

  /** Checks that a plan obeys all the invariants. */
  private static void noViolations(RelNode plan) {
    assertThat(violations(plan), empty());
  }

  /** Checks that the checker gives exactly one message for a plan. */
  private static void oneViolation(RelNode plan, String message) {
    assertThat(messages(plan), is(ImmutableList.of(message)));
  }

  @Test void testCorrelateIsValid() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = deptWithEmps(b, v, 1000);
    final String expectedPlan = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(plan, hasTree(expectedPlan));
    noViolations(plan);
    assertThat(CorrelateInvariantChecker.isValid(plan, Litmus.THROW), is(true));
  }

  /** The sub-query of a Filter uses the variable that the Filter declares:
   * {@code SELECT * FROM dept WHERE EXISTS (SELECT * FROM emp
   * WHERE emp.deptno = dept.deptno AND emp.sal > 1000)}. */
  @Test void testSubQueryOfFilterIsValid() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = b.scan("DEPT")
        .filter(ImmutableList.of(v.id),
            b.exists(b2 -> empsOfDept(b2, v, 1000)))
        .build();
    final String expectedPlan = ""
        + "LogicalFilter(condition=[EXISTS({\n"
        + "LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "})], variablesSet=[[$cor0]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(plan, hasTree(expectedPlan));
    noViolations(plan);
  }

  /** The sub-query in the ON condition of a Join reads a field of the left
   * input and a field of the right input:
   * {@code SELECT * FROM emp JOIN dept ON EXISTS (SELECT * FROM bonus
   * WHERE bonus.ename = emp.ename AND bonus.job = dept.dname)}. */
  @Test void testSubQueryOfJoinConditionIsValid() {
    final RelBuilder b = builder();
    // The variable refers to a row of EMP followed by a row of DEPT
    final RelDataType empThenDept =
        SqlValidatorUtil.createJoinType(b.getTypeFactory(),
            tableRowType(b, "EMP"), tableRowType(b, "DEPT"), null, ImmutableList.of());
    final RexCorrelVariable v = variable(b, empThenDept);
    final RelNode plan = b.scan("EMP").scan("DEPT")
        .join(JoinRelType.INNER,
            b.exists(b2 ->
                b2.scan("BONUS")
                    .filter(b2.equals(b2.field("ENAME"), b2.field(v, "ENAME")),
                        b2.equals(b2.field("JOB"), b2.field(v, "DNAME")))
                    .build()),
            ImmutableSet.of(v.id))
        .build();
    final String expectedPlan = ""
        + "LogicalJoin(condition=[EXISTS({\n"
        + "LogicalFilter(condition=[AND(=($0, $cor0.ENAME), =($1, $cor0.DNAME))])\n"
        + "  LogicalTableScan(table=[[scott, BONUS]])\n"
        + "})], joinType=[inner], variablesSet=[[$cor0]])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(plan, hasTree(expectedPlan));
    noViolations(plan);
  }

  /** Invariant 1 permits two declarations of one id if they are equal: one
   * node in two positions, or two nodes with equal contents. */
  @Test void testEqualDeclarationsOfOneIdAreValid() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode correlate = deptWithEmps(b, v, 1000);
    final RelNode sameNodeTwoTimes =
        b.push(correlate).push(correlate).union(true).build();
    // The text of the plan is the same for the two cases
    final String expectedPlan = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(sameNodeTwoTimes, hasTree(expectedPlan));
    noViolations(sameNodeTwoTimes);

    final RelNode equalCorrelate = deptWithEmps(b, v, 1000);
    assertThat(equalCorrelate == correlate, is(false));
    final RelNode twoEqualNodes =
        b.push(correlate).push(equalCorrelate).union(true).build();
    assertThat(twoEqualNodes, hasTree(expectedPlan));
    noViolations(twoEqualNodes);
  }

  /** Two Correlates that are not equal declare the same id. */
  @Test void testInvariant1TwoDeclarationsOfOneId() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = b.push(deptWithEmps(b, v, 1000))
        .push(deptWithEmps(b, v, 2000))
        .union(true)
        .build();
    final String expectedPlan = ""
        + "  LogicalUnion(all=[true])\n"
        + ">   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + ">   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 2000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan, "$cor0 is declared by LogicalCorrelate and by LogicalCorrelate");
    assertThat(CorrelateInvariantChecker.isValid(plan, Litmus.IGNORE),
        is(false));
  }

  /** A plan uses a variable, and no node declares it. */
  @Test void testInvariant2UseWithNoDeclaration() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = empsOfDept(b, v, 1000);
    final String expectedPlan = ""
        + "> LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan, "$cor0 is used in LogicalFilter, but no node declares it");
  }

  /** A sub-query uses a variable, and the Filter that has the sub-query
   * does not declare it. */
  @Test void testInvariant2UseInSubQueryWithNoDeclaration() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = b.scan("DEPT")
        .filter(b.exists(b2 -> empsOfDept(b2, v, 1000)))
        .build();
    final String expectedPlan = ""
        + "  LogicalFilter(condition=[EXISTS({\n"
        + "> LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  })])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan, "$cor0 is used in LogicalFilter, but no node declares it");
  }

  /** The left input and the right input of a Correlate use a variable that
   * no node declares. The Correlate declares a different variable. */
  @Test void testInvariant2UseWithNoDeclarationInInputsOfCorrelate() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RexCorrelVariable notDeclared = variable(b, tableRowType(b, "DEPT"));
    final RelNode left = b.scan("DEPT")
        .filter(b.equals(b.field("DEPTNO"), b.field(notDeclared, "DEPTNO")))
        .build();
    final RelNode right = b.scan("EMP")
        .filter(b.equals(b.field("DEPTNO"), b.field(v, "DEPTNO")),
            b.equals(b.field("MGR"), b.field(notDeclared, "DEPTNO")))
        .build();
    b.push(left).push(right);
    final RelNode plan =
        b.correlate(JoinRelType.INNER, v.id, b.field(2, 0, "DEPTNO")).build();
    final String expectedPlan = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + ">   LogicalFilter(condition=[=($0, $cor1.DEPTNO)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + ">   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), =($3, $cor1.DEPTNO))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    // One message for the Filter in each input
    final String message = "$cor1 is used in LogicalFilter, but no node declares it";
    assertThat(messages(plan), is(ImmutableList.of(message, message)));
  }

  /** A Filter above a Correlate uses the variable of the Correlate. The
   * scope of the variable is the right input of the Correlate only. */
  @Test void testInvariant2UseAboveDeclaration() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = b.push(deptWithEmps(b, v, 1000))
        .filter(b.equals(b.field("DEPTNO"), b.field(v, "DEPTNO")))
        .build();
    final String expectedPlan = ""
        + "> LogicalFilter(condition=[=($0, $cor0.DEPTNO)])\n"
        + ">   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan,
        "$cor0 is used in LogicalFilter, which is not in the scope of LogicalCorrelate");
  }

  /** A Filter uses its variable in its condition, not in a sub-query. */
  @Test void testInvariant2UseOutsideSubQuery() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = b.scan("DEPT")
        .filter(ImmutableList.of(v.id),
            b.equals(b.field("DEPTNO"), b.field(v, "DEPTNO")))
        .build();
    final String expectedPlan = ""
        + "> LogicalFilter(condition=[=($0, $cor0.DEPTNO)], variablesSet=[[$cor0]])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan, "$cor0 is used in LogicalFilter, but not in a sub-query of that node");
  }

  /** A Filter declares two variables, v1 and v2, and each of its two
   * sub-queries uses one of them. Each use is in scope and has the correct
   * type, thus a node with two declarations is the reported error. */
  @Test void testInvariant3TwoVariablesInOneNode() {
    final RelBuilder b = builder();
    final RexCorrelVariable v1 = variable(b, tableRowType(b, "DEPT"));
    final RexCorrelVariable v2 = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = b.scan("DEPT")
        .filter(ImmutableList.of(v1.id, v2.id),
            b.exists(b2 -> empsOfDept(b2, v1, 1000)),
            b.exists(b2 -> empsOfDept(b2, v2, 2000)))
        .build();
    final String expectedPlan = ""
        + "> LogicalFilter(condition=[AND(EXISTS({\n"
        + "  LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  }), EXISTS({\n"
        + "  LogicalFilter(condition=[AND(=($7, $cor1.DEPTNO), >($5, 2000))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  }))], variablesSet=[[$cor0, $cor1]])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan, "LogicalFilter declares 2 variables");
  }

  /** Invariant 4 permits a use whose type has more fields than the row, if
   * the field that the use reads is the same in the two types. */
  @Test void testUseTypeWithMoreFieldsIsValid() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, deptRowTypeWithExtraField(b));
    final RelNode plan = deptWithEmps(b, v, 1000);
    // The plan does not show the type of the use
    final String expectedPlan = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(plan, hasTree(expectedPlan));
    noViolations(plan);
  }

  /** A use reads field 1 as ENAME, a VARCHAR(10). The variable refers to a
   * row of DEPT, where field 1 is DNAME, a VARCHAR(14). */
  @Test void testInvariant4UseReadsFieldWithDifferentType() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "EMP"));
    final RelNode right = b.scan("EMP")
        .filter(b.equals(b.field("ENAME"), b.field(v, "ENAME")))
        .build();
    b.scan("DEPT").push(right);
    final RelNode plan =
        b.correlate(JoinRelType.INNER, v.id, b.field(2, 0, "DNAME")).build();
    final String expectedPlan = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + ">   LogicalFilter(condition=[=($1, $cor0.ENAME)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan,
        "Field 1 of $cor0 has type VARCHAR(10) in LogicalFilter, "
            + "but it has type VARCHAR(14) in the row");
  }

  /** A use reads field 7 (the DEPTNO of an EMP row). The variable refers to
   * a row of DEPT, which has 3 fields. The required columns of the Correlate
   * cannot contain field 7, thus the plan also does not obey
   * invariant 5. */
  @Test void testInvariant4UseReadsFieldThatRowDoesNotHave() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "EMP"));
    final RelNode plan = deptWithEmps(b, v, 1000);
    // The plan does not show the type of the use
    final String expectedPlan = ""
        + "> LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + ">   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    final List<String> expected =
        ImmutableList.of(
            "LogicalFilter reads field 7 of $cor0, but it refers to a row with 3 fields",
            "The right input of LogicalCorrelate reads fields {7} of $cor0, "
                + "but requiredColumns is {0}");
    assertThat(messages(plan), is(expected));
  }

  @Test void testFullRowUseIsValid() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = deptWithFullRowUse(b, v, "DEPTNO", "DNAME", "LOC");
    final String expectedPlan = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0, 1, 2}])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(EMPNO=[$0], D=[$cor0])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(plan, hasTree(expectedPlan));
    noViolations(plan);
  }

  /** A use reads the full row, and its type has one field more than the
   * row. */
  @Test void testInvariant4FullRowUseWithDifferentType() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, deptRowTypeWithExtraField(b));
    final RelNode plan = deptWithFullRowUse(b, v, "DEPTNO", "DNAME", "LOC");
    // The plan does not show the type of the use
    final String expectedPlan = ""
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0, 1, 2}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + ">   LogicalProject(EMPNO=[$0], D=[$cor0])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan,
        "$cor0 has type " + v.getType().getFullTypeString()
            + " in LogicalProject, but it refers to a row of type "
            + tableRowType(b, "DEPT").getFullTypeString());
  }

  /** The right input of a Correlate reads DEPTNO of the variable, and the
   * required columns of the Correlate are empty. */
  @Test void testInvariant5RequiredColumnsWithoutFieldThatUseReads() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode emps = empsOfDept(b, v, 1000);
    b.scan("DEPT").push(emps);
    final RelNode plan = b.correlate(JoinRelType.INNER, v.id).build();
    final String expectedPlan = ""
        + "> LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan,
        "The right input of LogicalCorrelate reads fields {0} of $cor0, "
            + "but requiredColumns is {}");
  }

  /** A use reads the full row, thus it reads all 3 fields of DEPT. The
   * required columns of the Correlate contain DEPTNO only. */
  @Test void testInvariant5RequiredColumnsWithFullRowUse() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode plan = deptWithFullRowUse(b, v, "DEPTNO");
    final String expectedPlan = ""
        + "> LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalProject(EMPNO=[$0], D=[$cor0])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan,
        "The right input of LogicalCorrelate reads fields {0, 1, 2} of $cor0, "
            + "but requiredColumns is {0}");
  }

  /** The right input of a Correlate is a Calc that reads DEPTNO of the
   * variable. The program of the Calc has the variable as one expression
   * ($t8), and DEPTNO of that expression as another ($t9). */
  @Test void testCalcThatReadsOneFieldIsValid() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    b.scan("EMP");
    final RexNode condition = b.equals(b.field("DEPTNO"), b.field(v, "DEPTNO"));
    final RelNode emp = b.build();
    final RexProgramBuilder program =
        new RexProgramBuilder(emp.getRowType(), b.getRexBuilder());
    program.addIdentity();
    program.addCondition(condition);
    b.scan("DEPT").push(LogicalCalc.create(emp, program.getProgram()));
    final RelNode plan =
        b.correlate(JoinRelType.INNER, v.id, b.field(2, 0, "DEPTNO")).build();
    final String expectedPlan = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalCalc(expr#0..7=[{inputs}], expr#8=[$cor0], expr#9=[$t8.DEPTNO], "
        + "expr#10=[=($t7, $t9)], proj#0..7=[{exprs}], $condition=[$t10])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(plan, hasTree(expectedPlan));
    noViolations(plan);
  }

  /** The right input of a Correlate is a Calc that projects the full row of
   * the variable, thus it reads all 3 fields of DEPT. The required columns
   * of the Correlate contain DEPTNO only. */
  @Test void testInvariant5RequiredColumnsWithFullRowUseInCalc() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    b.scan("EMP");
    final RexNode empno = b.field("EMPNO");
    final RelNode emp = b.build();
    final RexProgramBuilder program =
        new RexProgramBuilder(emp.getRowType(), b.getRexBuilder());
    program.addProject(empno, "EMPNO");
    program.addProject(v, "D");
    b.scan("DEPT").push(LogicalCalc.create(emp, program.getProgram()));
    final RelNode plan =
        b.correlate(JoinRelType.INNER, v.id, b.field(2, 0, "DEPTNO")).build();
    final String expectedPlan = ""
        + "> LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalCalc(expr#0..7=[{inputs}], expr#8=[$cor0], EMPNO=[$t0], D=[$t8])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan,
        "The right input of LogicalCorrelate reads fields {0, 1, 2} of $cor0, "
            + "but requiredColumns is {0}");
  }

  /** Returns a plan with {@code depth} Unions above a Correlate, and
   * {@code depth} Unions between the Correlate and a use of its variable. */
  private static RelNode correlateBetweenUnions(RelBuilder b, RexCorrelVariable v,
      int depth) {
    final RelNode emps = unionLadder(b, empsOfDept(b, v, 1000), depth);
    b.scan("DEPT").push(emps);
    final RelNode correlate =
        b.correlate(JoinRelType.INNER, v.id, b.field(2, 0, "DEPTNO")).build();
    return unionLadder(b, correlate, depth);
  }

  /** The plan is a DAG with 2^40 paths from its root to the Correlate, and
   * 2^40 paths from the Correlate to the use of its variable. The checker
   * visits each node one time. A checker that visits each path does not
   * complete. The first plan has 1 Union in place of 40, to show the
   * shape. */
  @Test void testDagWithManyPathsIsValid() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode oneUnion = correlateBetweenUnions(b, v, 1);
    final String expectedPlan = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalUnion(all=[true])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalUnion(all=[true])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + "      LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(oneUnion, hasTree(expectedPlan));
    noViolations(oneUnion);

    final RelNode plan = correlateBetweenUnions(b, v, 40);
    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> noViolations(plan));
  }

  /** One Filter that uses a variable is in two positions of the plan: in the
   * right input of the Correlate that declares the variable, and in the
   * right input of a Join above that Correlate. The first position is in
   * the scope of the variable, and the second position is not. */
  @Test void testInvariant2SharedNodeInScopeAndOutOfScope() {
    final RelBuilder b = builder();
    final RexCorrelVariable v = variable(b, tableRowType(b, "DEPT"));
    final RelNode emps = empsOfDept(b, v, 1000);
    b.scan("DEPT").push(emps);
    final RelNode correlate =
        b.correlate(JoinRelType.INNER, v.id, b.field(2, 0, "DEPTNO")).build();
    final RelNode plan = b.push(correlate).push(emps)
        .join(JoinRelType.INNER, b.literal(true))
        .build();
    final String expectedPlan = ""
        + "  LogicalJoin(condition=[true], joinType=[inner])\n"
        + ">   LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + ">     LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n"
        + ">   LogicalFilter(condition=[AND(=($7, $cor0.DEPTNO), >($5, 1000))])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(markedPlan(plan), is(expectedPlan));
    oneViolation(plan,
        "$cor0 is used in LogicalFilter, which is not in the scope of LogicalCorrelate");
  }
}
