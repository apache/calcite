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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.rules.AggregateFilterToFilteredAggregateRule;
import org.apache.calcite.rel.rules.CoreRules;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.rel.rules.CoreRules.AGGREGATE_FILTER_TO_FILTERED_AGGREGATE;
import static org.apache.calcite.rel.rules.CoreRules.AGGREGATE_PROJECT_MERGE;
import static org.apache.calcite.rel.rules.CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS;

/**
 * Unit test for {@link AggregateFilterToFilteredAggregateRule}.
 */
class AggregateFilterToFilteredAggregateRuleTest {

  private static RelOptFixture fixture() {
    return RelOptFixture.DEFAULT.withDiffRepos(
        DiffRepository.lookup(AggregateFilterToFilteredAggregateRuleTest.class));
  }

  private static RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  @Test void testSingleColumnAggregate() {
    String sql = "select sum(sal) from emp where deptno = 10";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE).check();
  }

  @Test void testSingleStarAggregate() {
    String sql = "select count(*) from emp where deptno = 10";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE).check();
  }

  @Test void testMultiAggregates() {
    String sql = "select sum(sal), min(sal), max(sal), count(*) from emp where deptno = 10";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE).check();
  }

  @Test void testSingleColumnFilteredAggregate() {
    String sql = "select sum(sal) filter (where ename = 'Bob') from emp where deptno = 10";
    List<RelOptRule> preRules = new ArrayList<>();
    preRules.add(AGGREGATE_PROJECT_MERGE);
    preRules.add(PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS);
    sql(sql).withPre(HepProgram.builder().addRuleCollection(preRules).build())
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE,
            CoreRules.PROJECT_MERGE).check();
  }

  @Test void testAggregateNoSupportingFilter() {
    String sql = "select single_value(sal) from emp where deptno = 10";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE)
        .checkUnchanged();
  }

  @Test void testSingleColumnAggregateWithGroupBy() {
    String sql = "select sum(sal) from emp where deptno = 10 group by job";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE)
        .checkUnchanged();
  }

  @Test void testSingleColumnAggregateWithGroupingSets() {
    String sql =
        "select sum(sal) from emp where deptno = 10 group by grouping sets ((job), (ename))";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE)
        .checkUnchanged();
  }

  @Test void testSingleColumnAggregateWithEmptyGroupBy() {
    String sql = "select sum(sal) from emp where deptno = 10 group by ()";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE).check();
  }

  @Test void testSingleColumnAggregateWithEmptyGroupingSets() {
    String sql = "select sum(sal) from emp where deptno = 10 group by grouping sets (())";
    sql(sql).withPreRule(AGGREGATE_PROJECT_MERGE)
        .withRule(AGGREGATE_FILTER_TO_FILTERED_AGGREGATE).check();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
