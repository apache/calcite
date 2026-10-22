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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.calcite.rel.rules.CoreRules.AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    // STDDEV_SAMP needs at least two non-null values to return 0.
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
    // VAR_SAMP needs at least two non-null values to return 0.
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
    // The count also distinguishes single-value groups, whose variance is NULL.
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
    // COUNT(expr) handles both NULL expressions and single-value groups.
    String sql = "select comm, stddev_samp(comm + 1) as sd\n"
        + "from empnullables group by comm";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testGlobalConstantAggregates() {
    String sql = "select min(1), max(1), avg(1), any_value(1),\n"
        + "stddev_pop(1), stddev_samp(1), var_pop(1), var_samp(1)\n"
        + "from emp where deptno < 0";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testGlobalConstantAggregatesWithOtherCalls() {
    String sql = "select sum(sal), min(1), count(*), stddev_samp(1), max(sal)\n"
        + "from emp";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testGlobalConstantAggregatesReuseCount() {
    String sql = "select count(*), min(1), stddev_samp(1) from emp";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testGlobalNullAggregates() {
    String sql = "select min(cast(null as integer)),\n"
        + "stddev_pop(cast(null as integer)),\n"
        + "stddev_samp(cast(null as integer)) from emp";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testGlobalDistinctConstantAggregates() {
    String sql = "select avg(distinct 1), stddev_pop(distinct 1),\n"
        + "stddev_samp(distinct 1), var_samp(distinct 1) from emp";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testDistinctStatisticalFunctionOfGroupByKey() {
    String sql = "select comm, stddev_samp(distinct comm), var_samp(distinct comm)\n"
        + "from empnullables group by comm";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testConstantAggregatesWithGroupBy() {
    String sql = "select deptno, min(1), stddev_pop(1), stddev_samp(1)\n"
        + "from emp group by deptno";
    sql(sql).withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).check();
  }

  @Test void testGlobalNonConstantAggregateNoChange() {
    sql("select min(sal), stddev_samp(sal) from emp")
        .withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  @Test void testFilteredConstantAggregateNoChange() {
    sql("select min(1) filter (where deptno < 0) from emp")
        .withRule(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).checkUnchanged();
  }

  static Stream<SqlAggFunction> nullOnEmptyFunctions() {
    return Stream.of(SqlStdOperatorTable.MIN, SqlStdOperatorTable.MAX,
        SqlStdOperatorTable.AVG, SqlStdOperatorTable.SUM,
        SqlStdOperatorTable.ANY_VALUE, SqlStdOperatorTable.STDDEV_POP,
        SqlStdOperatorTable.STDDEV_SAMP, SqlStdOperatorTable.VAR_POP,
        SqlStdOperatorTable.VAR_SAMP, SqlStdOperatorTable.LISTAGG,
        SqlLibraryOperators.STRING_AGG);
  }

  @ParameterizedTest
  @MethodSource("nullOnEmptyFunctions")
  void testNullEmptyResult(SqlAggFunction function) {
    final Aggregate aggregate = emptyAggregate(function);
    final RexNode result = function.getEmptyResult(aggregate.getCluster().getRexBuilder(),
        aggregate.getAggCallList().get(0));
    final RexLiteral literal = assertInstanceOf(RexLiteral.class, result);
    assertTrue(literal.isNull());
    assertThat(literal.getType(), is(aggregate.getAggCallList().get(0).getType()));
  }

  @Test void testZeroEmptyResultsPreserveType() {
    for (SqlAggFunction function
        : new SqlAggFunction[]{SqlStdOperatorTable.COUNT, SqlStdOperatorTable.SUM0}) {
      final Aggregate aggregate = emptyAggregate(function);
      final AggregateCall call = aggregate.getAggCallList().get(0);
      final RexNode result = function.getEmptyResult(aggregate.getCluster().getRexBuilder(), call);
      final RexLiteral literal = assertInstanceOf(RexLiteral.class, result);
      assertThat(literal.getValueAs(BigDecimal.class), comparesEqualTo(BigDecimal.ZERO));
      // COUNT returns BIGINT; SUM0 here returns DECIMAL with the operand's scale.
      assertThat(literal.getType(), is(call.getType()));
    }
  }

  @Test void testLiteralAggEmptyResult() {
    final RelBuilder b = builder();
    b.values(new String[]{"X"}, 1).empty();
    final Aggregate aggregate = (Aggregate) b.aggregate(b.groupKey(),
        b.literalAgg("literal").as("R")).build();
    final AggregateCall call = aggregate.getAggCallList().get(0);
    assertSame(call.rexList.get(0),
        call.getAggregation().getEmptyResult(b.getRexBuilder(), call));
  }

  @Test void testCollectEmptyResult() {
    final Aggregate aggregate = emptyAggregate(SqlStdOperatorTable.COLLECT);
    final AggregateCall call = aggregate.getAggCallList().get(0);
    final RexNode result = call.getAggregation()
        .getEmptyResult(aggregate.getCluster().getRexBuilder(), call);
    // Calcite's COLLECT returns an empty, non-null multiset on empty input.
    final RexLiteral literal = assertInstanceOf(RexLiteral.class, result);
    final List<?> values = literal.getValueAs(List.class);
    assertThat(values, hasSize(0));
    assertThat(literal.getType(), is(call.getType()));
  }

  @Test void testUnknownEmptyResult() {
    final SqlAggFunction function =
        SqlBasicAggFunction.create("CUSTOM", SqlKind.OTHER_FUNCTION,
            ReturnTypes.INTEGER_NULLABLE, OperandTypes.NUMERIC);
    final Aggregate aggregate = emptyAggregate(function);
    assertNull(function.getEmptyResult(aggregate.getCluster().getRexBuilder(),
        aggregate.getAggCallList().get(0)));
  }

  @Test void testUnknownEmptyResultPreventsReduction() {
    final SqlAggFunction function = new SqlAvgAggFunction(SqlKind.AVG) {
      @Override public @Nullable RexNode getEmptyResult(RexBuilder rexBuilder,
          AggregateCall call) {
        return null;
      }
    };
    final RelBuilder b = builder();
    b.values(new String[]{"X"}, 1, 2).project(b.literal(1));
    final RelNode original = b.aggregate(b.groupKey(),
        b.aggregateCall(function, b.field(0)).as("R")).build();
    assertThat(RelOptUtil.toString(reduce(original)), is(RelOptUtil.toString(original)));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void testEmptyResultDiffersFromInsufficientSample(int rowCount) throws Exception {
    final SqlAggFunction function = new SqlAvgAggFunction(SqlKind.STDDEV_SAMP) {
      @Override public RexNode getEmptyResult(RexBuilder rexBuilder, AggregateCall call) {
        return rexBuilder.makeLiteral(BigDecimal.TEN, call.getType());
      }
    };
    final RelBuilder b = builder();
    b.values(new String[]{"X"}, 1, 2)
        .filter(b.lessThanOrEqual(b.field(0), b.literal(rowCount)))
        .project(b.literal(1));
    final RelNode original = b.aggregate(b.groupKey(),
        b.aggregateCall(function, b.field(0)).as("R")).build();
    final RelNode reduced = reduce(original);
    assertThat(RelOptUtil.toString(reduced), not(containsString("STDDEV_SAMP(")));
    assertThat(reduced.getRowType(), is(original.getRowType()));
    try (PreparedStatement statement = RelRunners.run(reduced);
        ResultSet resultSet = statement.executeQuery()) {
      final String expected = rowCount == 0 ? "10" : rowCount == 1 ? "null" : "0";
      assertThat(CalciteAssert.toString(resultSet), is("R=" + expected + "\n"));
    }
  }

  @Test void testNullGroupUsesEmptyResult() throws Exception {
    final SqlAggFunction function = new SqlAvgAggFunction(SqlKind.AVG) {
      @Override public RexNode getEmptyResult(RexBuilder rexBuilder, AggregateCall call) {
        return rexBuilder.makeLiteral(BigDecimal.TEN, call.getType());
      }
    };
    final RelBuilder b = builder();
    b.values(new String[]{"X"}, 1, null);
    final RelNode original = b.aggregate(b.groupKey(b.field(0)),
        b.aggregateCall(function, b.field(0)).as("R")).build();
    final RelNode reduced = reduce(original);
    assertThat(RelOptUtil.toString(reduced), not(containsString("AVG(")));
    assertThat(reduced.getRowType(), is(original.getRowType()));
    try (PreparedStatement statement = RelRunners.run(reduced);
        ResultSet resultSet = statement.executeQuery()) {
      final String result = CalciteAssert.toString(resultSet);
      assertThat(result, containsString("X=null; R=10\n"));
      assertThat(result, containsString("X=1; R=1\n"));
    }
  }

  private static RelBuilder builder() {
    return RelBuilder.create(Frameworks.newConfigBuilder().build())
        .transform(c -> c.withAggregateUnique(true).withPruneInputOfAggregate(false)
            .withSimplify(false).withSimplifyValues(false));
  }

  private static Aggregate emptyAggregate(SqlAggFunction function) {
    final RelBuilder b = builder();
    b.values(new String[]{"N", "S"}, new BigDecimal("1.25"), "a").empty();
    final int arg = function == SqlStdOperatorTable.LISTAGG
        || function == SqlLibraryOperators.STRING_AGG ? 1 : 0;
    return (Aggregate) b.aggregate(b.groupKey(),
        b.aggregateCall(function, b.field(arg)).as("R")).build();
  }

  private static RelNode reduce(RelNode rel) {
    final HepPlanner planner = new HepPlanner(new HepProgramBuilder()
        .addRuleInstance(AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS).build());
    planner.setRoot(rel);
    return planner.findBestExp();
  }

  @AfterAll static void checkActualAndReferenceFiles() {
    fixture().diffRepos.checkActualAndReferenceFiles();
  }
}
