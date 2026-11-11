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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

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
   * <p>Extracting an expression must not replace a matching expression inside
   * a skipped lambda. Fields still referenced by the lambda must be retained. */
  @Test void testNonDeterministicLambda() throws Exception {
    final RelBuilder builder = RelBuilder.create(config().build());
    final Holder<@Nullable RexCorrelVariable> v = Holder.empty();
    builder.values(new String[] {"A", "B"}, 10, 20, 10, 20).variable(v::set);
    final RexLambdaRef x = new RexLambdaRef(0, "x", builder.field("A").getType());
    final RexNode pure =
        builder.call(SqlStdOperatorTable.PLUS, builder.field(v.get(), "A"), builder.literal(1));
    final RexNode sum =
        builder.call(SqlStdOperatorTable.PLUS,
            builder.call(SqlStdOperatorTable.PLUS, x, pure), builder.field(v.get(), "B"));
    final RexNode body =
        builder.lessThan(
            builder.call(SqlStdOperatorTable.PLUS, sum, builder.call(SqlStdOperatorTable.RAND)),
            builder.literal(3));
    final RexNode expression =
        builder.call(SqlLibraryOperators.EXISTS,
            builder.call(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                builder.literal(1), builder.literal(2)),
            builder.getRexBuilder().makeLambdaCall(body, ImmutableList.of(x)));
    final RelNode before = builder.values(new String[] {"V"}, 0, 1)
        .project(
            builder.alias(pure, "P"), builder.alias(expression, "Q"))
        .correlate(JoinRelType.INNER, v.get().id,
            builder.field(2, 0, "A"), builder.field(2, 0, "B"))
        .project(builder.field("P"), builder.field("Q"))
        .build();
    final RelNode after =
        before.accept(new CorrelateProjectExtractor(RelFactories.LOGICAL_BUILDER));
    final Correlate extracted = extractedCorrelate(after);
    assertThat(extracted.getRequiredColumns(), is(ImmutableBitSet.of(0, 1, 2)));
    assertThat(((Project) extracted.getRight()).getProjects().get(1), is(expression));
    final RelNode trimmed = new RelFieldTrimmer(null, builder).trim(after);
    try (PreparedStatement statement = RelRunners.run(trimmed)) {
      assertThat(CalciteAssert.toString(statement.executeQuery()),
          is("P=11; Q=false\nP=11; Q=false\nP=11; Q=false\nP=11; Q=false\n"));
    }
  }

  private static Correlate extractedCorrelate(RelNode rel) {
    while (!(rel instanceof Correlate)) {
      rel = rel.getInput(0);
    }
    return (Correlate) rel;
  }
}
