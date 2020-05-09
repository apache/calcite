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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.MatcherAssert.assertThat;

class RelFieldTrimmerTest {
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test public void testSortExchangeFieldTrimmer() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .sortExchange(RelDistributions.hash(Lists.newArrayList(1)), RelCollations.of(0))
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalSortExchange(distribution=[hash[1]], collation=[[0]])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test public void testSortExchangeFieldTrimmerWhenProjectCannotBeMerged() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .sortExchange(RelDistributions.hash(Lists.newArrayList(1)), RelCollations.of(0))
            .project(builder.field("EMPNO"))
            .build();

    RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalProject(EMPNO=[$0])\n"
        + "  LogicalSortExchange(distribution=[hash[1]], collation=[[0]])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test public void testSortExchangeFieldTrimmerWithEmptyCollation() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .sortExchange(RelDistributions.hash(Lists.newArrayList(1)), RelCollations.EMPTY)
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalSortExchange(distribution=[hash[1]], collation=[[]])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test public void testSortExchangeFieldTrimmerWithSingletonDistribution() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .sortExchange(RelDistributions.SINGLETON, RelCollations.of(0))
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalSortExchange(distribution=[single], collation=[[0]])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test public void testExchangeFieldTrimmer() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .exchange(RelDistributions.hash(Lists.newArrayList(1)))
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalExchange(distribution=[hash[1]])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test public void testExchangeFieldTrimmerWhenProjectCannotBeMerged() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .exchange(RelDistributions.hash(Lists.newArrayList(1)))
            .project(builder.field("EMPNO"))
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalProject(EMPNO=[$0])\n"
        + "  LogicalExchange(distribution=[hash[1]])\n"
        + "    LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

  @Test public void testExchangeFieldTrimmerWithSingletonDistribution() {
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode root =
        builder.scan("EMP")
            .project(builder.field("EMPNO"), builder.field("ENAME"), builder.field("DEPTNO"))
            .exchange(RelDistributions.SINGLETON)
            .project(builder.field("EMPNO"), builder.field("ENAME"))
            .build();

    final RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(null, builder);
    final RelNode trimmed = fieldTrimmer.trim(root);

    final String expected = ""
        + "LogicalExchange(distribution=[single])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(trimmed, hasTree(expected));
  }

}
