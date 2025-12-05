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

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.visualizer.RuleMatchVisualizer;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CoreRules;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Check the output of {@link RuleMatchVisualizer}.
 */
class RuleMatchVisualizerTest extends RelOptTestBase {

  @Nullable
  private static DiffRepository diffRepos = null;

  @AfterAll
  public static void checkActualAndReferenceFiles() {
    if (diffRepos != null) {
      diffRepos.checkActualAndReferenceFiles();
    }
  }

  @Override RelOptFixture fixture() {
    RelOptFixture fixture = super.fixture()
        .withDiffRepos(DiffRepository.lookup(RuleMatchVisualizerTest.class));
    diffRepos = fixture.diffRepos();
    return fixture;
  }

  @Test void testHepPlanner() {
    final String sql = "select a.name from dept a\n"
        + "union all\n"
        + "select b.name from dept b\n"
        + "order by name limit 10";

    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
        .addRuleInstance(CoreRules.SORT_UNION_TRANSPOSE)
        .build();
    HepPlanner planner = new HepPlanner(program);

    RuleMatchVisualizer viz = new RuleMatchVisualizer();
    viz.attachTo(planner);

    final RelOptFixture fixture = sql(sql).withPlanner(planner);
    fixture.check();

    String result = normalize(viz.getJsonStringResult());
    fixture.diffRepos().assertEquals("visualizer", "${visualizer}", result);
  }

  @Test void testVolcanoPlanner() {
    final String sql = "select a.name from dept a";

    VolcanoPlanner planner = new VolcanoPlanner();
    planner.setTopDownOpt(false);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    RelOptUtil.registerDefaultRules(planner, false, false);

    RuleMatchVisualizer viz = new RuleMatchVisualizer();
    viz.attachTo(planner);


    final RelOptFixture fixture = sql(sql)
        .withPlanner(planner)
        .withFactory(t ->
            t.withCluster(cluster ->
                RelOptCluster.create(planner, cluster.getRexBuilder())));
    fixture.check();

    String result = normalize(viz.getJsonStringResult());
    fixture.diffRepos().assertEquals("visualizer", "${visualizer}", result);
  }

  /**
   * Normalize the visualizer output, so that it is independent of other tests.
   */
  private String normalize(String str) {
    // rename rel ids
    str =
        renameMatches(str,
            Pattern.compile("\"([0-9]+)\"|"
                + "\"label\" *: *\"#([0-9]+)-|"
                + "\"label\" *: *\"subset#([0-9]+)-|"
                + "\"explanation\" *: *\"\\{subset=rel#([0-9]+):"),
            1000);
    // rename rule call ids
    str = renameMatches(str, Pattern.compile("\"id\" *: *\"([0-9]+)-"), 100);
    return str;
  }

  /**
   * Rename the first group of each match to a consecutive index, starting at the offset.
   */
  private String renameMatches(final String str,
      final Pattern pattern, int offset) {
    Map<String, String> rename = new HashMap<>();
    StringBuilder sb = new StringBuilder();
    Matcher m = pattern.matcher(str);

    int last = 0;
    while (m.find()) {
      int start = -1;
      int end = -1;
      String oldName = null;
      for (int i = 1; i <= m.groupCount(); i++) {
        if (m.group(i) != null) {
          oldName = m.group(i);
          start = m.start(i);
          end = m.end(i);
          break;
        }
      }
      requireNonNull(oldName, "oldName");
      String newName = rename.computeIfAbsent(oldName, k -> "" + (rename.size() + offset));
      sb.append(str, last, start);
      sb.append(newName);
      last = end;
    }
    sb.append(str.substring(last));
    return sb.toString();
  }

}
