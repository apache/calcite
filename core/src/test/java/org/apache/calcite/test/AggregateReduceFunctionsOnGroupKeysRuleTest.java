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

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7493">[CALCITE-7493]
   * Support constant-result aggregates (e.g., STDDEV_POP, STDDEV) over GROUP BY keys</a>. */
  @Test void testStatisticalFunctionStddevSampOfGroupByKey() {
    // STDDEV_SAMP of a constant (GROUP BY key) is 0.
    String sql = "select sal, stddev_samp(sal) as sd\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testStatisticalFunctionStddevPopOfGroupByKey() {
    // STDDEV_POP of a constant (GROUP BY key) is 0.
    String sql = "select sal, stddev_pop(sal) as sdp\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testStatisticalFunctionVarPopOfGroupByKey() {
    // VAR_POP of a constant (GROUP BY key) is 0.
    String sql = "select sal, var_pop(sal) as vp\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testStatisticalFunctionVarSampOfGroupByKey() {
    // VAR_SAMP of a constant (GROUP BY key) is 0
    // (variance of all identical values is 0).
    String sql = "select sal, var_samp(sal) as vs\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testMultipleStatisticalFunctions() {
    // Test multiple statistical functions together.
    String sql = "select sal, stddev_samp(sal) as sd, stddev_pop(sal) as sdp,\n"
        + "var_pop(sal) as vp, var_samp(sal) as vs\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testStatisticalFunctionWithBinaryExpression() {
    // Variance of binary expression (sal + deptno) where all operands are GROUP BY keys.
    // Since all values are constant within each group, variance is 0.
    String sql = "select sal, var_pop(sal + deptno) as vp\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testStatisticalFunctionWithConstantExpression() {
    // Variance of expression with constant (2*sal + 100) where sal is a GROUP BY key.
    // Since all values are constant within each group, variance is 0.
    String sql = "select sal, stddev_pop(2 * sal + 100) as sdp\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testStatisticalFunctionWithMultipleGroupByKeys() {
    // Variance of expression combining multiple GROUP BY keys (sal * deptno).
    // Since all values are constant within each group, variance is 0.
    String sql = "select sal, var_samp(sal * deptno) as vs\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testStatisticalFunctionWithNonGroupByColumnNoOptimization() {
    // Negative test: expression contains only non-GROUP BY column (comm).
    // The rule should NOT optimize because comm is not a constant within the group.
    String sql = "select sal, var_pop(comm) as vp\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @Test void testStatisticalFunctionWithMixedColumnsNoOptimization() {
    // Negative test: expression mixes GROUP BY column (sal) and non-GROUP BY column (comm).
    // The rule should NOT optimize because comm is not a constant within the group.
    String sql = "select sal, stddev_pop(sal + comm) as sdp\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @Test void testStatisticalFunctionWithPartialExpressionNoOptimization() {
    // Negative test: expression combines a GROUP BY key (sal) with non-GROUP BY column (empno).
    // The rule should NOT optimize because empno is not constant within the group.
    String sql = "select sal, var_samp(sal * empno) as vs\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @Test void testStatisticalFunctionWithComplexMixNoOptimization() {
    // Negative test: complex expression with only GROUP BY column (sal) but
    // also referencing non-GROUP BY column (comm) in multiplication.
    String sql = "select sal, stddev_samp(sal * 2 + comm) as sd\n"
        + "from emp group by sal, deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @Test void testStatisticalFunctionNullableGroupKey() {
    // Test NULL semantics: when GROUP BY key is nullable, STDDEV(key) should
    // handle NULL correctly. The optimization wraps result in
    // CASE WHEN key IS NULL THEN NULL ELSE 0 END
    String sql = "select comm, stddev_pop(comm) as sdp\n"
        + "from empnullables group by comm";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testStatisticalFunctionNullableExpression() {
    // Test NULL semantics for expressions: when expression is nullable,
    // STDDEV(expr) should return NULL when expr is NULL.
    // Optimization wraps result in CASE to preserve NULL semantics
    String sql = "select comm, stddev_samp(comm + 1) as sd\n"
        + "from empnullables group by comm";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
