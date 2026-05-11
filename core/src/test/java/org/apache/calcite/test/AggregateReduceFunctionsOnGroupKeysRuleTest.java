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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS;

/**
 * Unit tests for {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsOnGroupKeysRule}.
 *
 * <p>Relevant tickets:
 * <ul>
 * <li><a href="https://issues.apache.org/jira/browse/CALCITE-7484">
 * [CALCITE-7484] Add a rule to eliminate redundant aggregates functions over GROUP BY keys
 * </a></li>
 * </ul>
 */
class AggregateReduceFunctionsOnGroupKeysRuleTest {

  private static RelOptFixture fixture() {
    return RelOptFixture.DEFAULT.withDiffRepos(
        DiffRepository.lookup(AggregateReduceFunctionsOnGroupKeysRuleTest.class));
  }

  private static RelOptFixture sql(String sql) {
    return fixture().sql(sql);
  }

  @Test void testAggregateFunctionOfGroupByKeys() {
    String sql = "select sal, max(sal) as sal_max, min(sal) as sal_min,\n"
        + "avg(sal) sal_avg, any_value(sal) as sal_val\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysPartial() {
    String sql = "select sal, max(sal) as sal_max, sum(comm) as comm_sum\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysNoChange() {
    String sql = "select sal, max(comm) as comm_max\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
