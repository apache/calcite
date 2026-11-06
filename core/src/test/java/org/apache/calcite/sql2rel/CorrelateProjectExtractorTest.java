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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalConditionalCorrelate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.util.List;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link CorrelateProjectExtractor}.
 */
public class CorrelateProjectExtractorTest {
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null);
  }

  @Test void testSingleCorrelationCallOverVariableInFilter() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode before = builder.scan("EMP")
        .variable(v::set)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.call(
                    SqlStdOperatorTable.PLUS,
                    builder.literal(10),
                    builder.field(v.get(), "DEPTNO"))))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .build();

    final String planBefore = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, +(10, $cor0.DEPTNO))])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(before, hasTree(planBefore));

    RelNode after = before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    final String planAfter = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[$9], DNAME=[$10], LOC=[$11])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{8}])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+(10, $7)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[=($0, $cor0.$f8)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7646">[CALCITE-7646]
   * CorrelateProjectExtractor does not handle nested field accesses cor0.field0.field1</a>. */
  @Test void testNestedCorrelationFieldAccessInFilter() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode before = builder.scan("EMP")
        .project(
            builder.alias(
                builder.call(SqlStdOperatorTable.ROW,
                    builder.field("EMPNO"), builder.field("DEPTNO")), "R"))
        .variable(v::set)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.getRexBuilder().makeFieldAccess(builder.field(v.get(), "R"), 1)))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "R"))
        .build();

    final String planBefore = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
        + "  LogicalProject(R=[ROW($0, $7)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, $cor0.R.EXPR$1)])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(before, hasTree(planBefore));

    RelNode after = before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    final String planAfter = ""
        + "LogicalProject(R=[$0], DEPTNO=[$2], DNAME=[$3], LOC=[$4])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n"
        + "    LogicalProject(R=[ROW($0, $7)], $f1=[ROW($0, $7).EXPR$1])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[=($0, $cor0.$f1)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Tests that a constant call operand, such as {@code POWER(2, 3)}, does
   * not prevent extracting the enclosing correlated call. */
  @Test void testCorrelationCallWithConstantCallOperandInFilter() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode before = builder.scan("EMP")
        .variable(v::set)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field(0),
                builder.call(SqlStdOperatorTable.PLUS,
                    builder.call(SqlStdOperatorTable.POWER,
                        builder.literal(2), builder.literal(3)),
                    builder.field(v.get(), "DEPTNO"))))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .build();

    final String planBefore = ""
        + "LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($0, +(POWER(2, 3), $cor0.DEPTNO))])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(before, hasTree(planBefore));

    RelNode after = before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    final String planAfter = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[$9], DNAME=[$10], LOC=[$11])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{8}])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+(POWER(2, 3), $7)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[=($0, $cor0.$f8)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  @Test void testDoubleCorrelationCallOverVariableInFilters() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    RelNode before = builder
        .scan("EMP")
        .variable(v::set)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field("DEPTNO"),
                builder.call(SqlStdOperatorTable.PLUS,
                    builder.literal(10),
                    builder.field(v.get(), "DEPTNO"))))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .variable(v::set)
        .scan("DEPT")
        .filter(
            builder.equals(builder.field("DEPTNO"),
                builder.call(SqlStdOperatorTable.MINUS,
                    builder.literal(50), builder.field(v.get(), "DEPTNO"))))
        .correlate(JoinRelType.LEFT, v.get().id, builder.field(2, 0, "DEPTNO"))
        .build();

    final String planBefore = ""
        + "LogicalCorrelate(correlation=[$cor1], joinType=[left], requiredColumns=[{7}])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalFilter(condition=[=($0, +(10, $cor0.DEPTNO))])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalFilter(condition=[=($0, -(50, $cor1.DEPTNO))])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(before, hasTree(planBefore));

    RelNode after = before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    final String planAfter = ""
        + "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[$8], DNAME=[$9], LOC=[$10], DEPTNO1=[$12], DNAME0=[$13], LOC0=[$14])\n"
        + "  LogicalCorrelate(correlation=[$cor1], joinType=[left], requiredColumns=[{11}])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[$9], DNAME=[$10], LOC=[$11], $f11=[-(50, $7)])\n"
        + "      LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{8}])\n"
        + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+(10, $7)])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n"
        + "        LogicalFilter(condition=[=($0, $cor0.$f8)])\n"
        + "          LogicalTableScan(table=[[scott, DEPT]])\n"
        + "    LogicalFilter(condition=[=($0, $cor1.$f11)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    assertThat(after, hasTree(planAfter));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7914">[CALCITE-7914]
   * RelDecorrelator produces wrong results for non-deterministic inputs</a>.
   *
   * <p>Lift the deterministic A + 1 call without lifting fragments out of the
   * non-deterministic lambda. B remains a required outer field even though
   * the lambda stays on the right. */
  @Test void testNonDeterministicLambda() throws Exception {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    builder.values(new String[] {"A", "B"}, 10, 20).variable(v::set);
    final RexLambdaRef x = new RexLambdaRef(0, "x", builder.field("A").getType());
    final RexNode sum =
        builder.call(SqlStdOperatorTable.PLUS, x, builder.field(v.get(), "B"));
    final RexNode body =
        builder.lessThan(
            builder.call(SqlStdOperatorTable.PLUS, sum, builder.call(SqlStdOperatorTable.RAND)),
            builder.literal(3));
    final RexNode expression =
        builder.call(
            SqlLibraryOperators.EXISTS, builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            builder.literal(1), builder.literal(2)),
        builder.getRexBuilder().makeLambdaCall(body, ImmutableList.of(x)));
    final RelNode before = builder.values(new String[] {"V"}, 0)
        .project(
            builder.alias(
                builder.call(SqlStdOperatorTable.PLUS,
                builder.field(v.get(), "A"), builder.literal(1)), "P"),
            builder.alias(expression, "Q"))
        .correlate(JoinRelType.INNER, v.get().id,
            builder.field(2, 0, "A"), builder.field(2, 0, "B"))
        .project(builder.field("P"), builder.field("Q"))
        .build();
    final RelNode after =
        before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    assertThat(
        after, hasTree(""
        + "LogicalProject(P=[$2], Q=[$3])\n"
        + "  LogicalProject(A=[$0], B=[$1], P=[$3], Q=[$4])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1, 2}])\n"
        + "      LogicalProject(A=[$0], B=[$1], $f2=[+($0, 1)])\n"
        + "        LogicalValues(tuples=[[{ 10, 20 }]])\n"
        + "      LogicalProject(P=[$cor0.$f2], Q=[EXISTS(ARRAY(1, 2), (x) -> <(+(+(x, $cor0.B), RAND()), 3))])\n"
        + "        LogicalValues(tuples=[[{ 0 }]])\n"));
    final RelNode trimmed = new RelFieldTrimmer(null, builder).trim(after);
    try (PreparedStatement statement = RelRunners.run(trimmed)) {
      assertThat(CalciteAssert.toString(statement.executeQuery()), is("P=11; Q=false\n"));
    }
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7914">[CALCITE-7914]
   * RelDecorrelator produces wrong results for non-deterministic inputs</a>.
   *
   * <p>The outer and nested bindings deliberately share an id and row type.
   * Extracting the outer call must not replace the equal call in the nested
   * ConditionalCorrelate. */
  @Test void testNestedConditionalCorrelate() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final RelNode left = builder.values(new String[] {"A", "B"}, 10, 20).build();
    final RelNode innerLeft = builder.values(new String[] {"A", "B"}, 1, 2).build();
    final CorrelationId id = builder.getCluster().createCorrel();
    final RexNode outer = rexBuilder.makeCorrel(left.getRowType(), id);
    final RexNode inner = rexBuilder.makeCorrel(innerLeft.getRowType(), id);
    final RexNode innerExpression =
        builder.call(SqlStdOperatorTable.PLUS,
            rexBuilder.makeFieldAccess(inner, 0), builder.literal(1));
    final RexNode outerExpression =
        builder.call(SqlStdOperatorTable.PLUS,
            rexBuilder.makeFieldAccess(outer, 0), builder.literal(1));
    final RelNode innerRight = builder.values(new String[] {"X"}, 0)
        .project(builder.alias(innerExpression, "P")).build();
    final RelNode nested =
        LogicalConditionalCorrelate.create(innerLeft, innerRight, ImmutableList.of(),
            id, ImmutableBitSet.of(0), JoinRelType.LEFT_MARK, builder.literal(true));
    final RelNode right =
        LogicalProject.create(nested, ImmutableList.of(),
            ImmutableList.of(outerExpression, rexBuilder.makeInputRef(nested, 2)),
            ImmutableList.of("P", "Q"), ImmutableSet.of());
    final RelNode before =
        LogicalCorrelate.create(left, right, ImmutableList.of(),
            id, ImmutableBitSet.of(0), JoinRelType.LEFT);
    final RelNode after =
        before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    assertThat(
        after, hasTree(""
        + "LogicalProject(A=[$0], B=[$1], P=[$3], Q=[$4])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{2}])\n"
        + "    LogicalProject(A=[$0], B=[$1], $f2=[+($0, 1)])\n"
        + "      LogicalValues(tuples=[[{ 10, 20 }]])\n"
        + "    LogicalProject(P=[$cor0.$f2], Q=[$2])\n"
        + "      LogicalConditionalCorrelate(correlation=[$cor0], joinType=[left_mark], requiredColumns=[{0}])\n"
        + "        LogicalValues(tuples=[[{ 1, 2 }]])\n"
        + "        LogicalProject(P=[+($cor0.A, 1)])\n"
        + "          LogicalValues(tuples=[[{ 0 }]])\n"));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7914">[CALCITE-7914]
   * RelDecorrelator produces wrong results for non-deterministic inputs</a>.
   *
   * <p>After extracting an outer ROW field, an inner reference using the same
   * id must not keep the outer ROW in requiredColumns. Otherwise the unused
   * structured correlation prevents flattening. */
  @Test void testNestedCorrelationWithStructuredField() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final RelNode left = builder.values(new String[] {"A", "B"}, 1, 2)
        .project(
            builder.alias(
                builder.call(SqlStdOperatorTable.ROW,
            builder.field("A"), builder.field("B")), "R"))
        .build();
    final CorrelationId id = builder.getCluster().createCorrel();
    final RexNode outer = rexBuilder.makeCorrel(left.getRowType(), id);
    final RelNode innerLeft = builder.values(new String[] {"S"}, 3).build();
    final RexNode inner = rexBuilder.makeCorrel(innerLeft.getRowType(), id);
    final RelNode innerRight = builder.values(new String[] {"V"}, 0)
        .project(builder.alias(rexBuilder.makeFieldAccess(inner, 0), "Q")).build();
    final RelNode nested =
        LogicalCorrelate.create(innerLeft, innerRight, ImmutableList.of(),
            id, ImmutableBitSet.of(0), JoinRelType.INNER);
    final RexNode outerField =
        rexBuilder.makeFieldAccess(rexBuilder.makeFieldAccess(outer, 0), 1);
    final RelNode right =
        LogicalProject.create(nested, ImmutableList.of(),
            ImmutableList.of(outerField, rexBuilder.makeInputRef(nested, 1)),
            ImmutableList.of("P", "Q"), ImmutableSet.of());
    final RelNode before =
        LogicalCorrelate.create(left, right, ImmutableList.of(),
            id, ImmutableBitSet.of(0), JoinRelType.LEFT);
    final RelNode after =
        before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    final RelNode flattened =
        new RelStructuredTypeFlattener(builder, rexBuilder,
            ViewExpanders.simpleContext(builder.getCluster()), false).rewrite(after);
    assertThat(
        flattened, hasTree(""
        + "LogicalProject(R=[$0], R1=[$1], P=[$3], Q=[$4])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{2}])\n"
        + "    LogicalProject(R$$0=[$0], R$$1=[$1], $f1=[ROW($0, $1).EXPR$1])\n"
        + "      LogicalValues(tuples=[[{ 1, 2 }]])\n"
        + "    LogicalProject(P=[$cor0.$f1], Q=[$1])\n"
        + "      LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])\n"
        + "        LogicalValues(tuples=[[{ 3 }]])\n"
        + "        LogicalProject(Q=[$cor0.S])\n"
        + "          LogicalValues(tuples=[[{ 0 }]])\n"));
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7914">[CALCITE-7914]
   * RelDecorrelator produces wrong results for non-deterministic inputs</a>.
   *
   * <p>A Project or Filter binds its id in its own expressions, not its input.
   * Keep the local expression intact while extracting outer expressions above
   * the binding and in its input. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testLocalBindingExpressions(boolean projectBinding) {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RexBuilder rexBuilder = builder.getRexBuilder();
    final RelNode left = builder.values(new String[] {"A", "B"}, 10, 20).build();
    final CorrelationId id = builder.getCluster().createCorrel();
    final RexNode outer = rexBuilder.makeCorrel(left.getRowType(), id);
    final RelNode values = builder.values(new String[] {"A", "B"}, 22, 33).build();
    final RexNode inputExpression =
        builder.call(SqlStdOperatorTable.PLUS,
            rexBuilder.makeFieldAccess(outer, 1), builder.literal(2));
    final RelNode input =
        LogicalFilter.create(values,
            builder.equals(rexBuilder.makeInputRef(values, 0), inputExpression));
    final RexNode local = rexBuilder.makeCorrel(input.getRowType(), id);
    final RexNode localExpression =
        builder.call(SqlStdOperatorTable.PLUS,
            rexBuilder.makeFieldAccess(local, 0), builder.literal(1));
    final RelNode binder = projectBinding
        ? LogicalProject.create(input, ImmutableList.of(), ImmutableList.of(localExpression),
            ImmutableList.of("P"), ImmutableSet.of(id))
        : LogicalFilter.create(input, builder.greaterThan(localExpression, builder.literal(0)),
            ImmutableSet.of(id));
    final RexNode outerExpression =
        builder.call(SqlStdOperatorTable.PLUS,
            rexBuilder.makeFieldAccess(outer, 0), builder.literal(1));
    final RelNode right =
        LogicalProject.create(binder, ImmutableList.of(),
            ImmutableList.of(outerExpression, rexBuilder.makeInputRef(binder, 0)),
            ImmutableList.of("P", "Q"), ImmutableSet.of());
    final RelNode before =
        LogicalCorrelate.create(left, right, ImmutableList.of(),
            id, ImmutableBitSet.of(0, 1), JoinRelType.LEFT);
    final RelNode after =
        before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    final String binderTree = projectBinding
        ? "      LogicalProject(variablesSet=[[$cor0]], P=[+($cor0.A, 1)])\n"
        : "      LogicalFilter(condition=[>(+($cor0.A, 1), 0)], variablesSet=[[$cor0]])\n";
    assertThat(
        after, hasTree(""
        + "LogicalProject(A=[$0], B=[$1], P=[$4], Q=[$5])\n"
        + "  LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{2, 3}])\n"
        + "    LogicalProject(A=[$0], B=[$1], $f2=[+($0, 1)], $f3=[+($1, 2)])\n"
        + "      LogicalValues(tuples=[[{ 10, 20 }]])\n"
        + "    LogicalProject(P=[$cor0.$f2], Q=[$0])\n"
        + binderTree
        + "        LogicalFilter(condition=[=($0, $cor0.$f3)])\n"
        + "          LogicalValues(tuples=[[{ 22, 33 }]])\n"));
  }
}
