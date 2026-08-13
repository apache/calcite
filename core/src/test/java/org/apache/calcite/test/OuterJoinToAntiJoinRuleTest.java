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

import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.OuterJoinToAntiJoinRule;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OuterJoinToAntiJoinRule}.
 *
 * <p><a href="https://issues.apache.org/jira/browse/CALCITE-7711">[CALCITE-7711]
 * Add a rule to convert LEFT or RIGHT OUTER JOIN with IS NULL to ANTI JOIN</a>.
 */
class OuterJoinToAntiJoinRuleTest {

  private static RelOptFixture fixture() {
    return RelOptFixture.DEFAULT.withDiffRepos(
        DiffRepository.lookup(OuterJoinToAntiJoinRuleTest.class));
  }

  private static RelOptFixture sql(String sql) {
    return fixture().sql(sql)
        .withRule(CoreRules.OUTER_JOIN_TO_ANTI_JOIN);
  }

  @Test void testLeftJoin() {
    final String sql = "select e.empno, d.name\n"
        + "from emp e left join dept d on e.deptno = d.deptno\n"
        + "where d.deptno is null and e.empno > 10";
    sql(sql).check();
  }

  @Test void testNullableJoinKey() {
    final String sql = "select e.empno\n"
        + "from emp e left join deptnullables d on e.deptno = d.deptno\n"
        + "where d.deptno is null";
    sql(sql).check();
  }

  @Test void testRightJoin() {
    final String sql = "select e.ename, d.name\n"
        + "from emp e right join dept d on e.deptno = d.deptno\n"
        + "where e.empno is null";
    sql(sql).check();
  }

  @Test void testCorrelatedLeftJoin() {
    final String sql = "select e.empno\n"
        + "from emp e left join dept d\n"
        + "on e.deptno = d.deptno and exists (\n"
        + "  select 1 from dept d2 where d2.name = d.name)\n"
        + "where d.deptno is null";
    sql(sql).check();
  }

  @Test void testCorrelatedRightJoin() {
    final String sql = "select d.deptno\n"
        + "from emp e right join dept d\n"
        + "on e.deptno = d.deptno and exists (\n"
        + "  select 1 from dept d2 where d2.name = d.name)\n"
        + "where e.empno is null";
    sql(sql).checkUnchanged();
  }

  @Test void testNullableNonJoinColumn() {
    final String sql = "select e.empno\n"
        + "from emp e left join deptnullables d on e.deptno = d.deptno\n"
        + "where d.name is null";
    sql(sql).checkUnchanged();
  }

  @Test void testIsNullOnPreservedInput() {
    final String sql = "select e.empno\n"
        + "from emp e left join dept d on e.deptno = d.deptno\n"
        + "where e.comm is null";
    sql(sql).checkUnchanged();
  }

  @Test void testNullSafeJoinCondition() {
    final String sql = "select e.empno\n"
        + "from empnullables e left join deptnullables d\n"
        + "on e.deptno is not distinct from d.deptno\n"
        + "where d.deptno is null";
    sql(sql).checkUnchanged();
  }

  @Test void testIsNullInDisjunction() {
    final String sql = "select e.empno\n"
        + "from emp e left join dept d on e.deptno = d.deptno\n"
        + "where d.deptno is null or e.empno > 10";
    sql(sql).checkUnchanged();
  }

  @Test void testNonDeterministicFilter() {
    final String sql = "select e.empno\n"
        + "from emp e left join dept d on e.deptno = d.deptno\n"
        + "where d.deptno is null and rand() > 0.5";
    sql(sql).checkUnchanged();
  }

  @Test void testNonDeterministicJoinCondition() {
    final String sql = "select e.empno\n"
        + "from emp e left join dept d\n"
        + "on e.deptno = d.deptno and rand() > 0.5\n"
        + "where d.deptno is null";
    sql(sql).checkUnchanged();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
