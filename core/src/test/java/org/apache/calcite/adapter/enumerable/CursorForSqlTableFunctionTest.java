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
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.CursorInput;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests cursor input for explicitly registered SQL table functions. */
class CursorForSqlTableFunctionTest {
  private static final CursorFunction ECHO = new CursorFunction();

  /** Explicit SQL function, independent of reflection-based Java UDTFs. */
  public static class CursorFunction extends SqlFunction implements SqlTableFunction {
    CursorFunction() {
      super("CURSOR_ECHO", SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, null,
          OperandTypes.CURSOR, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
      return binding -> binding.getCursorOperand(0);
    }

    public static Enumerable<Object[]> echo(CursorInput input) {
      assertEquals("N", input.getRowType().getFieldNames().get(0));
      return input.rows();
    }
  }

  private static RexImplementorTable implementors() {
    return RexImplementorTables.chain(new RexImplementorTable() {
      @Override public @Nullable RexCallImplementor get(SqlOperator operator) {
        return operator == ECHO
            ? RexImpTable.wrapAsRexCallImplementor(
                RexImpTable.createImplementor(
                    (translator, call, operands) ->
                        Expressions.call(CursorFunction.class, "echo", operands.get(0)),
                    NullPolicy.NONE, false))
            : null;
      }

      @Override public @Nullable AggImplementor get(SqlAggFunction operator, boolean window) {
        return null;
      }

      @Override public @Nullable MatchImplementor get(SqlMatchFunction operator) {
        return null;
      }

      @Override public @Nullable TableFunctionCallImplementor get(SqlWindowTableFunction operator) {
        return null;
      }
    }, RexImpTable.instance());
  }

  @Test void sqlFunctionReceivesCursorInput() throws Exception {
    final List<RelOptRule> rules = new ArrayList<>(EnumerableRules.rules());
    rules.addAll(RelOptRules.CALC_RULES);
    rules.remove(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(Frameworks.createRootSchema(true))
        .operatorTable(
            SqlOperatorTables.chain(SqlStdOperatorTable.instance(),
            SqlOperatorTables.of(ECHO)))
        .context(Contexts.of(implementors()))
        .programs(Programs.ofRules(rules))
        .build();
    try (Planner planner = Frameworks.getPlanner(config)) {
      final SqlNode parsed = planner.parse("select * from table(CURSOR_ECHO("
          + "cursor(select * from (values (1, 'a'), (2, 'b')) as t(n, s))))");
      final RelNode logical = planner.rel(planner.validate(parsed)).project();
      final RelTraitSet traits = logical.getTraitSet().replace(EnumerableConvention.INSTANCE);
      final EnumerableRel physical = (EnumerableRel) planner.transform(0, traits, logical);
      final Map<String, Object> parameters = new HashMap<>();
      parameters.put("_rexImplementorTable", implementors());
      final Bindable<?> bindable =
              EnumerableInterpretable.toBindable(parameters,
                  CalcitePrepare.Dummy.getSparkHandler(false), physical,
                  EnumerableRel.Prefer.ARRAY);
      final List<String> rows = bindable.bind(DataContexts.of(parameters))
          .select(row -> Arrays.toString((Object[]) row)).toList();
      assertEquals(Arrays.asList("[1, a]", "[2, b]"), rows);
    }
  }
}
