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
