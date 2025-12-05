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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@code org.apache.calcite.adapter.pig} package.
 */
class PigAdapterTest extends AbstractPigTest {

  // Undo the %20 replacement of a space by URL
  public static final ImmutableMap<String, String> MODEL =
      ImmutableMap.of("model",
          Sources.of(PigAdapterTest.class.getResource("/model.json"))
              .file().getAbsolutePath());

  @Test void testScanAndFilter() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"t\" where \"tc0\" > 'abc'")
        .explainContains("PigToEnumerableConverter\n"
            + "  PigFilter(condition=[>($0, 'abc')])\n"
            + "    PigTableScan(table=[[PIG, t]])")
        .runs()
        .queryContains(
            pigScriptChecker("t = LOAD '"
                + getFullPathForTestDataFile("data.txt")
                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
                + "t = FILTER t BY (tc0 > 'abc');"));
  }

  @Test void testImplWithMultipleFilters() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"t\" where \"tc0\" > 'abc' and \"tc1\" = '3'")
        .explainContains("PigToEnumerableConverter\n"
            + "  PigFilter(condition=[AND(>($0, 'abc'), =($1, '3'))])\n"
            + "    PigTableScan(table=[[PIG, t]])")
        .runs()
        .queryContains(
            pigScriptChecker("t = LOAD '"
                + getFullPathForTestDataFile("data.txt")
                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
                + "t = FILTER t BY (tc0 > 'abc') AND (tc1 == '3');"));
  }

  @Test void testImplWithGroupByAndCount() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select count(\"tc1\") c from \"t\" group by \"tc0\"")
        .explainContains("PigToEnumerableConverter\n"
            + "    PigAggregate(group=[{0}], C=[COUNT($1)])\n"
            + "      PigTableScan(table=[[PIG, t]])")
        .runs()
        .queryContains(
            pigScriptChecker("t = LOAD '"
                + getFullPathForTestDataFile("data.txt")
                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
                + "t = GROUP t BY (tc0);\n"
                + "t = FOREACH t {\n"
                + "  GENERATE group AS tc0, COUNT(t.tc1) AS C;\n"
                + "};"));
  }

  @Test void testImplWithCountWithoutGroupBy() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select count(\"tc0\") c from \"t\"")
        .explainContains("PigToEnumerableConverter\n"
            + "  PigAggregate(group=[{}], C=[COUNT($0)])\n"
            + "    PigTableScan(table=[[PIG, t]])")
        .runs()
        .queryContains(
            pigScriptChecker("t = LOAD '"
                + getFullPathForTestDataFile("data.txt")
                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
                + "t = GROUP t ALL;\n"
                + "t = FOREACH t {\n"
                + "  GENERATE COUNT(t.tc0) AS C;\n"
                + "};"));
  }

  @Test void testImplWithGroupByMultipleFields() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"t\" group by \"tc1\", \"tc0\"")
        .explainContains("PigToEnumerableConverter\n"
            + "  PigAggregate(group=[{0, 1}])\n"
            + "    PigTableScan(table=[[PIG, t]])")
        .runs()
        .queryContains(
            pigScriptChecker("t = LOAD '"
                + getFullPathForTestDataFile("data.txt")
                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
                + "t = GROUP t BY (tc0, tc1);\n"
                + "t = FOREACH t {\n"
                + "  GENERATE group.tc0 AS tc0, group.tc1 AS tc1;\n"
                + "};"));
  }

  @Test void testImplWithGroupByCountDistinct() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select count(distinct \"tc0\") c from \"t\" group by \"tc1\"")
        .explainContains("PigToEnumerableConverter\n"
            + "    PigAggregate(group=[{1}], C=[COUNT(DISTINCT $0)])\n"
            + "      PigTableScan(table=[[PIG, t]])")
        .runs()
        .queryContains(
            pigScriptChecker("t = LOAD '"
                + getFullPathForTestDataFile("data.txt")
                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
                + "t = GROUP t BY (tc1);\n"
                + "t = FOREACH t {\n"
                + "  tc0_DISTINCT = DISTINCT t.tc0;\n"
                + "  GENERATE group AS tc1, COUNT(tc0_DISTINCT) AS C;\n"
                + "};"));
  }

  @Test void testImplWithJoin() {
    CalciteAssert.that()
        .with(MODEL)
        .query("select * from \"t\" join \"s\" on \"tc1\"=\"sc0\"")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE))
        .explainContains("PigToEnumerableConverter\n"
            + "  PigJoin(condition=[=($1, $2)], joinType=[inner])\n"
            + "    PigTableScan(table=[[PIG, t]])\n"
            + "    PigTableScan(table=[[PIG, s]])")
        .runs()
        .queryContains(
            pigScriptChecker("t = LOAD '"
                + getFullPathForTestDataFile("data.txt")
                + "' USING PigStorage() AS (tc0:chararray, tc1:chararray);\n"
                + "s = LOAD '" + getFullPathForTestDataFile("data2.txt")
                + "' USING PigStorage() AS (sc0:chararray, sc1:chararray);\n"
                + "t = JOIN t BY tc1 , s BY sc0;"));
  }

  /** Returns a function that checks that a particular Pig Latin scriptis
   * generated to implement a query. */
  @SuppressWarnings("rawtypes")
  private static Consumer<List> pigScriptChecker(final String... strings) {
    return actual -> {
      String actualArray =
          actual == null || actual.isEmpty()
              ? null
              : (String) actual.get(0);
      assertThat("expected Pig script not found", actualArray, is(strings[0]));
    };
  }
}
