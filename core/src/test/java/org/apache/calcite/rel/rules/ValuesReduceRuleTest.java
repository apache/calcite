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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link ValuesReduceRule}.
 */
class ValuesReduceRuleTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7450">[CALCITE-7450]
   * ValuesReduceRule incorrectly drops tuples when filter condition is
   * irreducible</a>.
   *
   * <p> {@code RAND()} function, is non-deterministic
   * therefore not reduced by {@code ReduceExpressionsRule}. */
  @Test void testFilterWithNonDeterministicConditionDoesNotDropTuples()
      throws Exception {
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(Frameworks.createRootSchema(true))
        .parserConfig(SqlParser.config().withCaseSensitive(false))
        .build();

    final Planner planner = Frameworks.getPlanner(config);
    final String sql = "SELECT * FROM (VALUES (0, 1, 2), (3, 4, 5)) "
        + "AS t(a, b, c) WHERE RAND(t.a) > 0.5";
    final RelNode planBefore =
        planner.rel(planner.validate(planner.parse(sql))).rel;

    final HepProgram program = HepProgram.builder()
        .addRuleInstance(CoreRules.PROJECT_FILTER_VALUES_MERGE)
        .build();
    final HepPlanner hepPlanner = new HepPlanner(program);
    hepPlanner.setRoot(planBefore);
    final RelNode planAfter = hepPlanner.findBestExp();

    // RAND() is non-deterministic, so the condition cannot be reduced.
    // The plan must remain unchanged.
    assertThat(RelOptUtil.toString(planAfter), is(RelOptUtil.toString(planBefore)));
  }
}
