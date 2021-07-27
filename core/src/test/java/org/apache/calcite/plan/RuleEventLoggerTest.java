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
package org.apache.calcite.plan;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Class to conveniently print the output of the {@link RuleEventLogger} with Volcano and
 * HepPlanner.
 *
 * The class is not meant to be committed.
 *
 * Add the following line in log4j.properties file and run the test:
 * <pre>
 *  log4j.logger.org.apache.calcite.plan.RelOptPlanner=DEBUG
 * </pre>
 */
public class RuleEventLoggerTest {
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT));
  }

  private RelNode createRootPlan() {
    RelBuilder builder = RelBuilder.create(config().build());
    // Equivalent SQL:
    //   SELECT e.ename
    //   FROM emp AS e, dept
    //   WHERE e.deptno = dept.deptno
    return builder
        .scan("EMP").as("e")
        .scan("DEPT")
        .join(JoinRelType.LEFT)
        .filter(
            builder.equals(
            builder.field("e", "DEPTNO"),
            builder.field("DEPT", "DEPTNO")))
        .project(builder.field("e", "ENAME"))
        .build();
  }

  @Test void testWithVolcano() {
    RelNode initPlan = createRootPlan();
    VolcanoPlanner planner = (VolcanoPlanner) initPlan.getCluster().getPlanner();
    planner.clear();
    planner.setNoneConventionHasInfiniteCost(false);
    planner.addRule(CoreRules.FILTER_INTO_JOIN);
    planner.addRule(CoreRules.PROJECT_JOIN_TRANSPOSE);
    planner.addRule(CoreRules.PROJECT_MERGE);
    planner.setRoot(initPlan);
    planner.findBestExp();
  }

  @Test void testWithHep() {
    RelNode initPlan = createRootPlan();
    HepProgram program = HepProgram.builder().addRuleCollection(
        Arrays.asList(
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.PROJECT_JOIN_TRANSPOSE,
        CoreRules.PROJECT_MERGE
    )).build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(initPlan);
    planner.findBestExp();
  }
}
