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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link org.apache.calcite.rel.rules.JoinAggregateTransposeRule}.
 *
 * <p>Relevant tickets:
 * <ul>
 * <li><a href="https://issues.apache.org/jira/browse/CALCITE-7604">
 * [CALCITE-7604] Add rule to pull up GROUP BY above JOIN
 * </a></li>
 * </ul>
 */
class JoinAggregateTransposeRuleTest {

  private static RelOptFixture fixture() {
    // Use SCOTT schema to keep unit and end-to-end (Quidem) tests aligned.
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT);
    CalciteConnectionConfig config =
        CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    return RelOptFixture.DEFAULT
        .withCatalogReaderFactory(
            (typeFactory, caseSensitive) ->
            new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                ImmutableList.of("SCOTT"),
                typeFactory,
                config))
        .withDiffRepos(DiffRepository.lookup(JoinAggregateTransposeRuleTest.class));
  }

  private static RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  /**
   * Tests that the rule can pull the group by from the left side of the join
   * in the trivial case where there are no aggregate functions.
   */
  @Test void testPullGroupByWithoutAggregateFunctions() {
    final String sql = "select g.deptno\n"
        + "from (select deptno from emp group by deptno) g\n"
        + "join dept d on g.deptno = d.deptno";
    sql(sql).withRule(CoreRules.JOIN_AGGREGATE_TRANSPOSE).check();
  }

  /**
   * Tests that the rule can pull the group by from the left side of the join
   * when that is a simple aggregation.
   */
  @Test void testPullGroupByFromLeftWithSimpleAggregation() {
    final String sql = "select g.deptno, g.total_sal, d.dname\n"
        + "from (select deptno, sum(sal) as total_sal\n"
        + "      from emp group by deptno) g\n"
        + "join dept d on g.deptno = d.deptno";
    sql(sql).withRule(CoreRules.JOIN_AGGREGATE_TRANSPOSE).check();
  }

  /**
   * Tests that the rule can pull the group by from the left side of the join
   * when that is a simple aggregation with multiple aggregate functions.
   */
  @Test void testPullGroupByFromLeftWithSimpleAggregationMultipleFunctions() {
    final String sql = "select g.deptno, g.total_sal, g.low_sal, g.high_sal, d.dname\n"
        + "from (select deptno, sum(sal) as total_sal, min(sal) as low_sal, max(sal) as high_sal\n"
        + "      from emp group by deptno) g\n"
        + "join dept d on g.deptno = d.deptno";
    sql(sql).withRule(CoreRules.JOIN_AGGREGATE_TRANSPOSE).check();
  }

  /**
   * Tests that the rule can pull the group by from the right side of the join
   * by exploiting the join commutativity. Demonstrates that there is no
   * need for implementing separate rule/logic for pulling the group by from the
   * right side of the join.
   */
  @Test void testPullGroupByFromRightWithSimpleAggregation() {
    final String sql = "select g.deptno, g.total_sal, d.dname\n"
        + "from dept d\n"
        + "join (select deptno, sum(sal) as total_sal\n"
        + "      from emp group by deptno) g\n"
        + "  on g.deptno = d.deptno";
    HepProgram program = HepProgram.builder()
        // Without a limit here the commute rule would keep flipping the
        // join indefinitely causing stack overflow.
        .addMatchLimit(1)
        .addRuleInstance(CoreRules.JOIN_COMMUTE)
        .addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT)
        .addRuleInstance(CoreRules.JOIN_AGGREGATE_TRANSPOSE)
        .build();
    sql(sql)
        .withProgram(program)
        .check();
  }

  /**
   * Tests that the rule can pull the group by from the left side of the join
   * even when there is a filtering on the right side.
   */
  @Test void testPullGroupByFromLeftWithFilterOnRight() {
    final String sql = "select g.deptno, g.total_sal, d.dname\n"
        + "from (select deptno, sum(sal) as total_sal\n"
        + "      from emp group by deptno) g\n"
        + "join dept d on g.deptno = d.deptno\n"
        + "where d.dname = 'RESEARCH'";
    sql(sql)
        .withPreRule(CoreRules.FILTER_INTO_JOIN)
        .withRule(CoreRules.JOIN_AGGREGATE_TRANSPOSE)
        .check();
  }

  /**
   * Tests that the rules not apply when the join is not an equijoin.
   */
  @Test void testNoPullGroupByAboveNonEquiJoin() {
    final String sql = "select g.deptno\n"
        + "from (select deptno from emp group by deptno) g\n"
        + "join dept d on g.deptno > d.deptno";
    sql(sql).withRule(CoreRules.JOIN_AGGREGATE_TRANSPOSE).checkUnchanged();
  }

  /**
   * Tests that the rule does not apply when the right join keys are not unique.
   * The job column is not unique thus the rule bails out.
   */
  @Test void testNoPullGroupByWhenRightJoinKeysNotUnique() {
    final String sql = "select g.job, g.cnt\n"
        + "from (select job, count(*) as cnt\n"
        + "      from emp group by job) g\n"
        + "join emp e on g.job = e.job";
    sql(sql).withRule(CoreRules.JOIN_AGGREGATE_TRANSPOSE).checkUnchanged();
  }


  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
