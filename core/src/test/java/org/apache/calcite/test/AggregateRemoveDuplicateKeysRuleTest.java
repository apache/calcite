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

import org.apache.calcite.rel.rules.AggregateRemoveDuplicateKeysRule;
import org.apache.calcite.rel.rules.CoreRules;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AggregateRemoveDuplicateKeysRule}.
 *
 * <p>Relevant tickets:
 * <ul>
 * <li><a href="https://issues.apache.org/jira/browse/CALCITE-7479">
 * [CALCITE-7479] Remove redundant aggregate group keys with FD
 * </a></li>
 * </ul>
 */
class AggregateRemoveDuplicateKeysRuleTest {

  private static RelOptFixture fixture() {
    return RelOptFixture.DEFAULT.withDiffRepos(
        DiffRepository.lookup(AggregateRemoveDuplicateKeysRuleTest.class));
  }

  private static RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  @Test void testRemoveOneRedundantGroupKey() {
    final String sql = "select deptno, name, count(*) as c\n"
        + "from sales.dept\n"
        + "group by deptno, name";

    sql(sql).withRule(CoreRules.AGGREGATE_REMOVE_DUPLICATE_KEYS)
        .check();
  }

  @Test void testRemoveMultipleRedundantGroupKeys() {
    final String sql = "select empno, ename, job, count(*) as c\n"
        + "from emp\n"
        + "group by empno, ename, job";

    sql(sql).withRule(CoreRules.AGGREGATE_REMOVE_DUPLICATE_KEYS)
        .check();
  }

  @Test void testRemoveRedundantComputedGroupKey() {
    // deptno + 2 is a deterministic function of deptno, so it is determined
    // by deptno and can be removed from the GROUP BY.
    final String sql = "select deptno, deptno + 2\n"
        + "from emp\n"
        + "group by deptno, deptno + 2";

    sql(sql).withRule(CoreRules.AGGREGATE_REMOVE_DUPLICATE_KEYS)
        .check();
  }

  @Test void testKeepsGroupKeyFromPreservedSideOfLeftJoin() {
    // The null-generating right key cannot determine the preserved left key:
    // unmatched rows with different e.deptno values all have d.deptno = NULL.
    final String sql = "select d.deptno, e.deptno, count(*) as c\n"
        + "from emp e\n"
        + "left join dept d on e.deptno = d.deptno\n"
        + "group by d.deptno, e.deptno";

    sql(sql).withRule(CoreRules.AGGREGATE_REMOVE_DUPLICATE_KEYS)
        .checkUnchanged();
  }

  @Test void testKeepsFdFromNullGeneratingInputOfLeftJoin() {
    // This FD comes from the right input rather than from the join condition.
    // The right Project has a -> COALESCE(a, 1), but null padding adds a row
    // with a = NULL and y = NULL alongside the matched a = NULL, y = 1 row.
    final String sql = "select r.a, r.y, count(*) as c\n"
        + "from (values (1), (2)) as l(z)\n"
        + "left join (\n"
        + "  select z, a, coalesce(a, 1) as y\n"
        + "  from (values (1, cast(null as integer))) as v(z, a)\n"
        + ") as r on l.z = r.z\n"
        + "group by r.a, r.y";

    sql(sql).withRule(CoreRules.AGGREGATE_REMOVE_DUPLICATE_KEYS)
        .checkUnchanged();
  }

  @Test void testMapsFdForNonContiguousAggregateGroupSet() {
    // AggregateProjectMergeRule changes the input group set to {1, 2, 3},
    // whose keys occupy output positions {0, 1, 2}. Map the input FD 1 -> 2
    // to output FD 0 -> 1, so the rule removes b rather than c.
    final String sql = "select a, b, c, count(*) as n\n"
        + "from (values (0, 1, 1, 10), (0, 1, 1, 20))\n"
        + "  as t(z, a, b, c)\n"
        + "where a = b\n"
        + "group by a, b, c";

    sql(sql).withPreRule(CoreRules.AGGREGATE_PROJECT_MERGE)
        .withRule(CoreRules.AGGREGATE_REMOVE_DUPLICATE_KEYS)
        .check();
  }

  @Test void testKeepsDoubleGroupKeyInferredFromEquality() {
    // SQL equality considers 0.0 and -0.0 equal, but Enumerable grouping
    // distinguishes their Double keys. Therefore x = y does not prove that
    // x determines y for the purpose of removing y from the GROUP BY.
    final String sql = "select x, y, count(*) as c\n"
        + "from (values\n"
        + "  (cast(0 as double), cast(0 as double)),\n"
        + "  (cast(0 as double), -cast(0 as double))) as t(x, y)\n"
        + "where x = y\n"
        + "group by x, y";

    sql(sql).withRule(CoreRules.AGGREGATE_REMOVE_DUPLICATE_KEYS)
        .checkUnchanged();
  }

  @Test void testKeepsNonRedundantGroupKeys() {
    final String sql = "select deptno, job, count(*) as c\n"
        + "from emp\n"
        + "group by deptno, job";

    sql(sql).withRule(CoreRules.AGGREGATE_REMOVE_DUPLICATE_KEYS)
        .checkUnchanged();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
