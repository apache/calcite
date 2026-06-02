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

  @Test void testAggregateFunctionOfGroupByKeysNullExpression() {
    String sql = "select comm, max(comm + 1) as max_plus\n"
        + "from empnullables group by comm";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysNullGroupKey() {
    String sql = "select comm, max(comm) as comm_max\n"
        + "from empnullables group by comm";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysNoChange() {
    String sql = "select sal, max(comm) as comm_max\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @Test void testAggregateFunctionOfGroupByKeysDeterministicExpression() {
    String sql = "select sal, max(sal + 1) as max_plus\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysUnaryMinus() {
    String sql = "select sal, max(-sal) as max_neg\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysBinaryExpression() {
    String sql = "select sal, max(sal * 2) as max_double\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysMultipleGroupKeys() {
    String sql = "select sal, max(sal + deptno) as max_sum\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysNestedExpression() {
    // Nested expressions like (sal + 1) * 2 can be optimized by mapping the
    // input references (sal) to group key references. The shuttle translates
    // all input refs in the expression to their corresponding group keys.
    String sql = "select sal, max((sal + 1) * 2) as max_expr\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysWithCastWider() {
    // Test case where a cast is needed because the group key type differs
    // from the aggregate result type. Cast to a wider type (BIGINT) is safe.
    // The rule should preserve the cast.
    String sql = "select cast(sal as bigint) as sal_big, max(sal) as sal_max\n"
        + "from emp group by sal";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionOfGroupByKeysWithCastNarrower() {
    // Test case where a cast is needed and the type is narrower than the source.
    // Casting to SMALLINT could potentially lose information if sal has larger values,
    // but this is the user's explicit choice. The rule should still optimize and
    // preserve the cast, allowing SQL semantics to handle any data loss.
    String sql = "select cast(sal as smallint) as sal_small, max(sal) as sal_max\n"
        + "from emp group by sal";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testAggregateFunctionWithMixedColumnsNoOptimization() {
    // Negative test: expression references both group-by and non-group-by columns.
    // The rule should NOT optimize because the expression is not constant within the group.
    String sql = "select sal, max(sal + comm) as max_sum\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @Test void testAggregateFunctionWithNonGroupByColumnNoOptimization() {
    // Negative test: expression references only non-group-by columns.
    // The rule should NOT optimize because the column is not in the GROUP BY set.
    String sql = "select sal, max(comm) as comm_max\n"
        + "from emp group by sal";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @Test void testAggregateFunctionWithMixedGroupByColumnsNoOptimization() {
    // Negative test: expression contains GROUP BY column but also references
    // a column from elsewhere that is not in GROUP BY.
    String sql = "select sal, max(sal + empno) as max_sum\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
