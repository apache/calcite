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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.test.Matchers.hasTree;

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
}
