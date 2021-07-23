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
package org.apache.calcite.materialize;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.SqlToRelTestBase;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class LogicalCorrelateMaterializationTest extends SqlToRelTestBase {

  private static final RelBuilder BUILDER = RelBuilder.create(config().build());

  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("mv0", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("empno", SqlTypeName.INTEGER)
            .add("num", SqlTypeName.INTEGER)
            .build();
      }
    });
    rootSchema.add("mv1", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("empno", SqlTypeName.INTEGER)
            .add("ename", SqlTypeName.VARCHAR)
            .add("num", SqlTypeName.INTEGER)
            .build();
      }
    });
    rootSchema.add("mv2", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("deptno", SqlTypeName.INTEGER)
            .add("name", SqlTypeName.VARCHAR)
            .add("name0", SqlTypeName.VARCHAR)
            .build();
      }
    });
    rootSchema.add("mv3", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("empno", SqlTypeName.INTEGER)
            .add("ename", SqlTypeName.VARCHAR)
            .add("deptno", SqlTypeName.INTEGER)
            .build();
      }
    });
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(rootSchema)
        .traitDefs((List<RelTraitDef>) null);
  }

  @Test void testCorrelateOnLeftCalcToCorrelate0() {
    final String target = ""
        + "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv0").build();
    final String dql = ""
        + "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num) where empno > 0";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv0"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t0, $t2)], proj#0."
        + ".1=[{exprs}], $condition=[$t3])\n"
        + "  LogicalTableScan(table=[[mv0]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnLeftCalcToCorrelate1() {
    final String target = ""
        + "select empno, ename, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv1").build();
    final String dql = "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));

    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));
    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], EMPNO=[$t0], NUM=[$t2])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], num=[$2])\n"
        + "    LogicalTableScan(table=[[mv1]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnLeftCalcToCorrelate2() {
    final String target = ""
        + "select empno, ename, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv1").build();
    final String dql = ""
        + "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num) where empno > 0";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[>($t0, $t3)], EMPNO=[$t0], "
        + "NUM=[$t2], $condition=[$t4])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], num=[$2])\n"
        + "    LogicalTableScan(table=[[mv1]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnLeftCalcToCorrelate3() {
    final String target = ""
        + "select empno, ename, r.num from emp join lateral table(ramp(emp.deptno)) as r(num) "
        + "on emp.deptno = num";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv1").build();
    final String dql = ""
        + "select empno, r.deptno1 from emp join lateral table(ramp(emp.deptno)) as r(deptno1) "
        + "on emp.deptno = r.deptno1 where empno > 0 ";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[>($t0, $t3)], EMPNO=[$t0], "
        + "DEPTNO1=[$t2], $condition=[$t4])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], num=[$2])\n"
        + "    LogicalTableScan(table=[[mv1]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnLeftCalcToCorrelate4() {
    final String target = ""
        + "select empno, ename, r.num from emp join lateral table(ramp(emp.deptno)) "
        + "as r(num) on emp.deptno = num";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv1").build();
    final String dql = ""
        + "select empno, r.num from emp join lateral table(ramp(emp.deptno)) as r(num) "
        + "on emp.deptno = num where empno > 0 ";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[>($t0, $t3)], EMPNO=[$t0], "
        + "NUM=[$t2], $condition=[$t4])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], num=[$2])\n"
        + "    LogicalTableScan(table=[[mv1]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnLeftCalcToCorrelate5() {
    final String target = ""
        + "select empno, ename, r.num from emp join lateral table(ramp(emp.deptno)) as r(num) "
        + "on emp.deptno = num and empno > 0";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv1").build();
    final String dql = ""
        + "select empno, r.num from emp join lateral table(ramp(emp.deptno)) as r(num) "
        + "on emp.deptno = num and empno > 0 where empno > 1 ";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[1], expr#4=[>($t0, $t3)], EMPNO=[$t0], "
        + "NUM=[$t2], $condition=[$t4])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], num=[$2])\n"
        + "    LogicalTableScan(table=[[mv1]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnLeftCalcToCorrelate6() {
    final String target = ""
        + "select empno, ename, deptno from emp where deptno "
        + "in (select deptno from dept where dept.deptno = emp.deptno)";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv3").build();
    final String dql = ""
        + "select empno, ename from emp where deptno "
        + "in (select deptno from dept where dept.deptno = emp.deptno) and deptno > 100";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv3"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[100], expr#4=[>($t2, $t3)], proj#0."
        + ".1=[{exprs}], $condition=[$t4])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], deptno=[$2])\n"
        + "    LogicalTableScan(table=[[mv3]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnLeftCalcToCorrelate7() {
    final String target = ""
        + "select empno, ename, deptno from emp "
        + "where exists (select deptno from dept where dept.deptno = emp.deptno)";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv3").build();
    final String dql = ""
        + "select empno, ename from emp "
        + "where exists (select deptno from dept where dept.deptno = emp.deptno) and deptno > 500";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv3"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[500], expr#4=[>($t2, $t3)], proj#0."
        + ".1=[{exprs}], $condition=[$t4])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], deptno=[$2])\n"
        + "    LogicalTableScan(table=[[mv3]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnRightCalcToCorrelate() {
    final String target = ""
        + "select empno, ename, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv1").build();
    final String dql = ""
        + "select empno, ename, r.num from emp, lateral table(ramp(emp.deptno)) as r(num) "
        + "where r.num > 0";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[>($t2, $t3)], proj#0."
        + ".2=[{exprs}], $condition=[$t4])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], num=[$2])\n"
        + "    LogicalTableScan(table=[[mv1]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testCorrelateOnCalcsToCorrelate() {
    final String target = ""
        + "select empno, ename, r.num from emp, lateral table(ramp(emp.deptno)) as r(num)";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv1").build();
    final String dql = ""
        + "select empno, r.num from emp, lateral table(ramp(emp.deptno)) as r(num) where r.num > 0";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv1"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[>($t2, $t3)], EMPNO=[$t0], "
        + "NUM=[$t2], $condition=[$t4])\n"
        + "  LogicalProject(empno=[$0], ename=[CAST($1):VARCHAR(20) NOT NULL], num=[$2])\n"
        + "    LogicalTableScan(table=[[mv1]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

  @Test void testLateraltableWithMutilArgs() {
    final String target = ""
        + "select * from dept, lateral table(DEDUP(dept.deptno, dept.name))";
    final RelNode mv = tester.convertSqlToRel(target).rel;
    final RelNode replacement = BUILDER.scan("mv2").build();
    final String dql = ""
        + "select deptno from dept, lateral table(DEDUP(dept.deptno, dept.name)) where deptno > 10";
    final RelNode query = tester.convertSqlToRel(dql).rel;
    final RelOptMaterialization relOptMaterialization = new RelOptMaterialization(replacement, mv,
        null, Lists.newArrayList("mv2"));
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized = RelOptMaterializations
        .useMaterializedViews(query, ImmutableList.of(relOptMaterialization));

    final String optimized = ""
        + "LogicalCalc(expr#0..2=[{inputs}], expr#3=[10], expr#4=[>($t0, $t3)], DEPTNO=[$t0], "
        + "$condition=[$t4])\n"
        + "  LogicalProject(deptno=[$0], name=[CAST($1):VARCHAR(10) NOT NULL], name0=[CAST($2)"
        + ":VARCHAR(1024) NOT NULL])\n"
        + "    LogicalTableScan(table=[[mv2]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(isLinux(optimized).matches(relOptimizedStr), is(true));
  }

}
