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

import org.apache.calcite.adapter.pig.PigAggregate;
import org.apache.calcite.adapter.pig.PigFilter;
import org.apache.calcite.adapter.pig.PigRel;
import org.apache.calcite.adapter.pig.PigRelFactories;
import org.apache.calcite.adapter.pig.PigRules;
import org.apache.calcite.adapter.pig.PigTable;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.TestUtil;

import org.apache.hadoop.fs.Path;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.test.Util;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.apache.calcite.rel.rules.FilterJoinRule.TRUE_PREDICATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@code org.apache.calcite.adapter.pig} package that tests the
 * building of {@link PigRel} relational expressions using {@link RelBuilder} and
 * associated factories in {@link PigRelFactories}.
 */
public class PigRelBuilderStyleTest extends AbstractPigTest {

  public PigRelBuilderStyleTest() {
    Assume.assumeThat("Pigs don't like Windows", File.separatorChar, is('/'));
  }

  @Test
  public void testScanAndFilter() throws Exception {
    final SchemaPlus schema = createTestSchema();
    final RelBuilder builder = createRelBuilder(schema);
    final RelNode node = builder.scan("t")
        .filter(builder.call(GREATER_THAN, builder.field("tc0"), builder.literal("abc"))).build();
    final RelNode optimized = optimizeWithVolcano(node);
    assertScriptAndResults("t", getPigScript(optimized, schema),
        "t = LOAD 'target/data.txt"
            + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
            + "t = FILTER t BY (tc0 > 'abc');",
        new String[] { "(b,2)", "(c,3)" });
  }

  @Test
  @Ignore("CALCITE-1751")
  public void testImplWithMultipleFilters() {
    final SchemaPlus schema = createTestSchema();
    final RelBuilder builder = createRelBuilder(schema);
    final RelNode node = builder.scan("t")
        .filter(
            builder.and(builder.call(GREATER_THAN, builder.field("tc0"), builder.literal("abc")),
                builder.call(EQUALS, builder.field("tc1"), builder.literal("3"))))
        .build();
    final RelNode optimized = optimizeWithVolcano(node);
    assertScriptAndResults("t", getPigScript(optimized, schema),
        "t = LOAD 'target/data.txt"
            + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
            + "t = FILTER t BY (tc0 > 'abc') AND (tc1 == '3');",
        new String[] { "(c,3)" });
  }

  @Test
  @Ignore("CALCITE-1751")
  public void testImplWithGroupByAndCount() {
    final SchemaPlus schema = createTestSchema();
    final RelBuilder builder = createRelBuilder(schema);
    final RelNode node = builder.scan("t")
        .aggregate(builder.groupKey("tc0"), builder.count(false, "c", builder.field("tc1")))
        .build();
    final RelNode optimized = optimizeWithVolcano(node);
    assertScriptAndResults("t", getPigScript(optimized, schema),
        "t = LOAD 'target/data.txt"
            + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
            + "t = GROUP t BY (tc0);\n"
            + "t = FOREACH t {\n"
            + "  GENERATE group AS tc0, COUNT(t.tc1) AS c;\n"
            + "};",
        new String[] { "(a,1)", "(b,1)", "(c,1)" });
  }

  @Test
  public void testImplWithCountWithoutGroupBy() {
    final SchemaPlus schema = createTestSchema();
    final RelBuilder builder = createRelBuilder(schema);
    final RelNode node = builder.scan("t")
        .aggregate(builder.groupKey(), builder.count(false, "c", builder.field("tc0"))).build();
    final RelNode optimized = optimizeWithVolcano(node);
    assertScriptAndResults("t", getPigScript(optimized, schema),
        "t = LOAD 'target/data.txt"
            + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
            + "t = GROUP t ALL;\n"
            + "t = FOREACH t {\n"
            + "  GENERATE COUNT(t.tc0) AS c;\n"
            + "};",
        new String[] { "(3)" });
  }

  @Test
  @Ignore("CALCITE-1751")
  public void testImplWithGroupByMultipleFields() {
    final SchemaPlus schema = createTestSchema();
    final RelBuilder builder = createRelBuilder(schema);
    final RelNode node = builder.scan("t")
        .aggregate(builder.groupKey("tc1", "tc0"), builder.count(false, "c", builder.field("tc1")))
        .build();
    final RelNode optimized = optimizeWithVolcano(node);
    assertScriptAndResults("t", getPigScript(optimized, schema),
        "t = LOAD 'target/data.txt"
            + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
            + "t = GROUP t BY (tc0, tc1);\n"
            + "t = FOREACH t {\n"
            + "  GENERATE group.tc0 AS tc0, group.tc1 AS tc1, COUNT(t.tc1) AS c;\n"
            + "};",
        new String[] { "(a,1,1)", "(b,2,1)", "(c,3,1)" });
  }

  @Test
  public void testImplWithGroupByCountDistinct() {
    final SchemaPlus schema = createTestSchema();
    final RelBuilder builder = createRelBuilder(schema);
    final RelNode node = builder.scan("t")
        .aggregate(builder.groupKey("tc1", "tc0"), builder.count(true, "c", builder.field("tc1")))
        .build();
    final RelNode optimized = optimizeWithVolcano(node);
    assertScriptAndResults("t", getPigScript(optimized, schema),
        "t = LOAD 'target/data.txt"
            + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
            + "t = GROUP t BY (tc0, tc1);\n"
            + "t = FOREACH t {\n"
            + "  tc1_DISTINCT = DISTINCT t.tc1;\n"
            + "  GENERATE group.tc0 AS tc0, group.tc1 AS tc1, COUNT(tc1_DISTINCT) AS c;\n"
            + "};",
        new String[] { "(a,1,1)", "(b,2,1)", "(c,3,1)" });
  }

  @Test
  public void testImplWithJoin() throws Exception {
    final SchemaPlus schema = createTestSchema();
    final RelBuilder builder = createRelBuilder(schema);
    final RelNode node = builder.scan("t").scan("s")
        .join(JoinRelType.INNER,
            builder.equals(builder.field(2, 0, "tc1"), builder.field(2, 1, "sc0")))
        .filter(builder.call(GREATER_THAN, builder.field("tc0"), builder.literal("a"))).build();
    final RelNode optimized = optimizeWithVolcano(node);
    assertScriptAndResults("t", getPigScript(optimized, schema),
        "t = LOAD 'target/data.txt"
            + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
            + "t = FILTER t BY (tc0 > 'a');\n"
            + "s = LOAD 'target/data2.txt"
            + "' USING PigStorage() AS (sc0:chararray, sc1:chararray);\n"
            + "t = JOIN t BY tc1 , s BY sc0;",
        new String[] { "(b,2,2,label2)" });
  }

  @Test
  @Ignore("CALCITE-1751")
  public void testImplWithJoinAndGroupBy() throws Exception {
    final SchemaPlus schema = createTestSchema();
    final RelBuilder builder = createRelBuilder(schema);
    final RelNode node = builder.scan("t").scan("s")
        .join(JoinRelType.LEFT,
            builder.equals(builder.field(2, 0, "tc1"), builder.field(2, 1, "sc0")))
        .filter(builder.call(GREATER_THAN, builder.field("tc0"), builder.literal("abc")))
        .aggregate(builder.groupKey("tc1"), builder.count(false, "c", builder.field("sc1")))
        .build();
    final RelNode optimized = optimizeWithVolcano(node);
    assertScriptAndResults("t", getPigScript(optimized, schema),
        "t = LOAD 'target/data.txt"
            + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
            + "t = FILTER t BY (tc0 > 'abc');\n"
            + "s = LOAD 'target/data2.txt"
            + "' USING PigStorage() AS (sc0:chararray, sc1:chararray);\n"
            + "t = JOIN t BY tc1 LEFT, s BY sc0;\n"
            + "t = GROUP t BY (tc1);\n"
            + "t = FOREACH t {\n"
            + "  GENERATE group AS tc1, COUNT(t.sc1) AS c;\n"
            + "};",
        new String[] { "(2,1)", "(3,0)" });
  }

  private SchemaPlus createTestSchema() {
    SchemaPlus result = Frameworks.createRootSchema(false);
    result.add("t",
        new PigTable("target/data.txt",
        new String[] { "tc0", "tc1" }));
    result.add("s",
        new PigTable("target/data2.txt",
        new String[] { "sc0", "sc1" }));
    return result;
  }

  private RelBuilder createRelBuilder(SchemaPlus schema) {
    final FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(schema)
        .context(PigRelFactories.ALL_PIG_REL_FACTORIES)
        .build();
    return RelBuilder.create(config);
  }

  private RelNode optimizeWithVolcano(RelNode root) {
    RelOptPlanner planner = getVolcanoPlanner(root);
    return planner.findBestExp();
  }

  private RelOptPlanner getVolcanoPlanner(RelNode root) {
    final RelBuilderFactory builderFactory =
        RelBuilder.proto(PigRelFactories.ALL_PIG_REL_FACTORIES);
    final RelOptPlanner planner = root.getCluster().getPlanner(); // VolcanoPlanner
    for (RelOptRule r : PigRules.ALL_PIG_OPT_RULES) {
      planner.addRule(r);
    }
    planner.removeRule(FilterAggregateTransposeRule.INSTANCE);
    planner.removeRule(FilterJoinRule.FILTER_ON_JOIN);
    planner.addRule(
        new FilterAggregateTransposeRule(PigFilter.class, builderFactory,
            PigAggregate.class));
    planner.addRule(
        new FilterIntoJoinRule(true, builderFactory, TRUE_PREDICATE));
    planner.setRoot(root);
    return planner;
  }

  private void assertScriptAndResults(String relAliasForStore, String script,
      String expectedScript, String[] expectedResults) {
    try {
      assertEquals(expectedScript, script);
      script = script + "\nSTORE " + relAliasForStore + " INTO 'myoutput';";
      PigTest pigTest = new PigTest(script.split("[\\r\\n]+"));
      pigTest.assertOutputAnyOrder(expectedResults);
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }
  }

  private String getPigScript(RelNode root, Schema schema) {
    PigRel.Implementor impl = new PigRel.Implementor();
    impl.visitChild(0, root);
    return impl.getScript();
  }

  @After
  public void shutdownPigServer() {
    PigServer pigServer = PigTest.getPigServer();
    if (pigServer != null) {
      pigServer.shutdown();
    }
  }

  @Before
  public void setupDataFilesForPigServer() throws Exception {
    System.getProperties().setProperty("pigunit.exectype",
        Util.getLocalTestMode().toString());
    Cluster cluster = PigTest.getCluster();
    // Put the data files in target/ so they don't dirty the local git checkout
    cluster.update(
        new Path(getFullPathForTestDataFile("data.txt")),
        new Path("target/data.txt"));
    cluster.update(
        new Path(getFullPathForTestDataFile("data2.txt")),
        new Path("target/data2.txt"));
  }
}

// End PigRelBuilderStyleTest.java
