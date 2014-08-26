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
package org.eigenbase.test;

import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;

import com.google.common.collect.Lists;

import static org.junit.Assert.*;

/**
 * RelOptTestBase is an abstract base for tests which exercise a planner and/or
 * rules via {@link DiffRepository}.
 */
abstract class RelOptTestBase extends SqlToRelTestBase {
  //~ Methods ----------------------------------------------------------------

  @Override protected Tester createTester() {
    return super.createTester().withDecorrelation(false);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given rule.
   *
   * @param rule Planner rule
   * @param sql  SQL query
   */
  protected void checkPlanning(
      RelOptRule rule,
      String sql) {
    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(rule);

    checkPlanning(
        programBuilder.build(),
        sql);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given
   * program.
   *
   * @param program Planner program
   * @param sql     SQL query
   */
  protected void checkPlanning(
      HepProgram program,
      String sql) {
    checkPlanning(
        new HepPlanner(program),
        sql);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given
   * planner.
   *
   * @param planner Planner
   * @param sql     SQL query
   */
  protected void checkPlanning(
      RelOptPlanner planner,
      String sql) {
    checkPlanning(tester, null, planner, sql);
  }

  /**
   * Checks the plan for a SQL statement before/after executing a given rule,
   * with a pre-program to prepare the tree.
   *
   * @param tester     Tester
   * @param preProgram Program to execute before comparing before state
   * @param planner    Planner
   * @param sql        SQL query
   */
  protected void checkPlanning(
      Tester tester,
      HepProgram preProgram,
      RelOptPlanner planner,
      String sql) {
    final DiffRepository diffRepos = getDiffRepos();
    String sql2 = diffRepos.expand("sql", sql);
    RelNode relInitial = tester.convertSqlToRel(sql2);

    assertTrue(relInitial != null);

    List<RelMetadataProvider> list = Lists.newArrayList();
    DefaultRelMetadataProvider defaultProvider =
        new DefaultRelMetadataProvider();
    list.add(defaultProvider);
    planner.registerMetadataProviders(list);
    RelMetadataProvider plannerChain =
        ChainedRelMetadataProvider.of(list);
    relInitial.getCluster().setMetadataProvider(plannerChain);

    RelNode relBefore;
    if (preProgram == null) {
      relBefore = relInitial;
    } else {
      HepPlanner prePlanner = new HepPlanner(preProgram);
      prePlanner.setRoot(relInitial);
      relBefore = prePlanner.findBestExp();
    }

    assertTrue(relBefore != null);

    String planBefore = NL + RelOptUtil.toString(relBefore);
    diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

    planner.setRoot(relBefore);
    RelNode relAfter = planner.findBestExp();

    String planAfter = NL + RelOptUtil.toString(relAfter);
    diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
  }
}

// End RelOptTestBase.java
