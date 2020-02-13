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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Verifies digest for {@link LogicalProject}.
 */
public class LogicalProjectDigestTest {
  /**
   * Planner does not compare
   */
  @Test public void fieldNamesDoNotInfluenceDigest() {
    final RelBuilder rb = RelBuilder.create(Frameworks.newConfigBuilder().build());
    final RelNode xAsEmpid = rb.values(new String[]{"x", "y", "z"}, 1, 2, 3)
        .project(
            rb.alias(rb.field("x"), "renamed_x"),
            rb.alias(rb.field("y"), "renamed_y"),
            rb.alias(rb.literal("u"), "extra_field"))
        .build();

    assertThat(
        "project column name should not be included to the project digest",
        RelOptUtil.toString(xAsEmpid, SqlExplainLevel.DIGEST_ATTRIBUTES),
        isLinux(""
            + "LogicalProject(inputs=[0..1], exprs=[['u']])\n"
            + "  LogicalValues(type=[RecordType(INTEGER x, INTEGER y, INTEGER z)], tuples=[[{ 1, 2, 3 }]])\n"));

    assertThat(
        "project column names should be present in EXPPLAN_ATTRIBUTES",
        RelOptUtil.toString(xAsEmpid, SqlExplainLevel.EXPPLAN_ATTRIBUTES),
        isLinux(""
            + "LogicalProject(renamed_x=[$0], renamed_y=[$1], extra_field=['u'])\n"
            + "  LogicalValues(tuples=[[{ 1, 2, 3 }]])\n"));

    assertThat(
        "project column names should be present with default RelOptUtil.toString(...)",
        RelOptUtil.toString(xAsEmpid),
        isLinux(""
            + "LogicalProject(renamed_x=[$0], renamed_y=[$1], extra_field=['u'])\n"
            + "  LogicalValues(tuples=[[{ 1, 2, 3 }]])\n"));
  }

  @Test public void testProjectDigestWithOneTrivialField() {
    final FrameworkConfig config = RelBuilderTest.config().build();
    final RelBuilder builder = RelBuilder.create(config);
    final RelNode rel = builder
        .scan("EMP")
        .project(builder.field("EMPNO"))
        .build();
    String digest = RelOptUtil.toString(rel, SqlExplainLevel.DIGEST_ATTRIBUTES);
    final String expected = ""
        + "LogicalProject(inputs=[0])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(digest, isLinux(expected));
  }
}
