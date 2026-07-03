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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.RexImpTable.RexCallImplementor;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;

import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static java.util.Objects.requireNonNull;

/**
 * Tests that an aggregate implementor supplied by a custom
 * {@link RexImplementorTable} registered on the planner
 * {@link org.apache.calcite.plan.Context} is used both to plan and to execute
 * an {@link EnumerableAggregate}.
 */
class EnumerableCustomAggregateTest {
  /** An aggregate function with no built-in implementor. */
  private static final SqlAggFunction MY_AGG =
      SqlBasicAggFunction.create("MY_CUSTOM_AGG", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BIGINT, OperandTypes.ANY);

  private static final String SQL = "SELECT g, MY_CUSTOM_AGG(x) AS c\n"
      + "FROM (VALUES ('a', 1), ('a', 2), ('b', 3)) AS t (g, x)\n"
      + "GROUP BY g";

  /** Returns a table that maps {@link #MY_AGG} to the built-in {@code COUNT}
   * implementor, so the custom aggregate counts its (non-null) input. */
  private static RexImplementorTable myAggTable() {
    final AggImplementor countImplementor =
        requireNonNull(
            RexImpTable.instance().get(SqlStdOperatorTable.COUNT, false),
            "COUNT implementor");
    return new RexImplementorTable() {
      @Override public @Nullable RexCallImplementor get(SqlOperator operator) {
        return null;
      }

      @Override public @Nullable AggImplementor get(SqlAggFunction aggregation,
          boolean forWindowAggregate) {
        return aggregation == MY_AGG ? countImplementor : null;
      }

      @Override public @Nullable MatchImplementor get(SqlMatchFunction function) {
        return null;
      }

      @Override public @Nullable TableFunctionCallImplementor get(
          SqlWindowTableFunction operator) {
        return null;
      }
    };
  }

  /** Plans a {@code GROUP BY} query through the {@link Frameworks} API. */
  private static RelNode planWithFrameworks(
      @Nullable RexImplementorTable implementorTable) throws Exception {
    final List<RelOptRule> rules = new ArrayList<>(EnumerableRules.rules());
    rules.addAll(RelOptRules.CALC_RULES);
    rules.remove(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(Frameworks.createRootSchema(true))
        .operatorTable(
            SqlOperatorTables.chain(SqlStdOperatorTable.instance(),
                SqlOperatorTables.of(MY_AGG)))
        .programs(Programs.ofRules(rules));
    if (implementorTable != null) {
      configBuilder = configBuilder.context(Contexts.of(implementorTable));
    }
    final FrameworkConfig config = configBuilder.build();

    final Planner planner = Frameworks.getPlanner(config);
    final SqlNode parse = planner.parse(SQL);
    final RelNode logical = planner.rel(planner.validate(parse)).project();
    final RelTraitSet traitSet =
        logical.getTraitSet().replace(EnumerableConvention.INSTANCE);
    return planner.transform(0, traitSet, logical);
  }

  private static EnumerableRel planDirectly(RexImplementorTable implementorTable)
      throws Exception {
    final VolcanoPlanner planner = new VolcanoPlanner(Contexts.of(implementorTable));
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    for (RelOptRule rule : EnumerableRules.rules()) {
      planner.addRule(rule);
    }
    for (RelOptRule rule : RelOptRules.CALC_RULES) {
      planner.addRule(rule);
    }

    final SqlTestFactory factory = SqlTestFactory.INSTANCE
        .withPlannerFactory(context -> planner)
        .withOperatorTable(opTab ->
            SqlOperatorTables.chain(opTab, SqlOperatorTables.of(MY_AGG)));
    final SqlNode parse = factory.createParser(SQL).parseQuery();
    final SqlToRelConverter converter = factory.createSqlToRelConverter();
    final SqlValidator validator = converter.validator;
    final RelRoot root =
        converter.convertQuery(validator.validate(parse), false, true);
    final RelTraitSet traitSet =
        root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode rel = planner.changeTraits(root.rel, traitSet);
    planner.setRoot(rel);
    return (EnumerableRel) planner.findBestExp();
  }

  private static Map<String, Long> execute(EnumerableRel physical) {
    final Bindable bindable =
        EnumerableInterpretable.toBindable(Collections.emptyMap(),
            CalcitePrepare.Dummy.getSparkHandler(false),
            physical, EnumerableRel.Prefer.ARRAY);
    final Enumerable<Object[]> result = bindable.bind(DataContexts.EMPTY);
    final Map<String, Long> actual = new HashMap<>();
    try (Enumerator<Object[]> enumerator = result.enumerator()) {
      while (enumerator.moveNext()) {
        final Object[] row = enumerator.current();
        actual.put((String) row[0], ((Number) row[1]).longValue());
      }
    }
    return actual;
  }

  /** With the custom table on the planner context, the aggregate is planned as
   * an {@link EnumerableAggregate} and executes to the expected values. */
  @Test void customAggregateImplementorIsUsedEndToEnd() throws Exception {
    final RelNode physical =
        planWithFrameworks(
            RexImplementorTables.chain(myAggTable(), RexImpTable.instance()));
    assertThat(RelOptUtil.toString(physical),
        containsString("EnumerableAggregate"));

    final Map<String, Long> actual = execute((EnumerableRel) physical);
    assertThat(actual, is(ImmutableMap.of("a", 2L, "b", 1L)));
  }

  @Test void directPlannerUsesCustomTable() throws Exception {
    final RexImplementorTable implementors =
        RexImplementorTables.chain(myAggTable(), RexImpTable.instance());
    final EnumerableRel physical = planDirectly(implementors);

    assertThat(RelOptUtil.toString(physical),
        containsString("EnumerableAggregate"));
    assertThat(execute(physical), is(ImmutableMap.of("a", 2L, "b", 1L)));
  }

  /** Without the custom table the built-in table has no implementor for the
   * aggregate, so {@link EnumerableAggregateRule} declines and planning fails. */
  @Test void unknownAggregateIsRejectedWithoutCustomTable() {
    assertThrows(RuntimeException.class,
        () -> planWithFrameworks(null));
  }
}
