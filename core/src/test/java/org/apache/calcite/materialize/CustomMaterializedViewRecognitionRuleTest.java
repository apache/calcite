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
package org.apache.calcite.materialize;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.SubstitutionVisitor.UnifyRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.mutable.MutableCalc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.SqlToRelTestBase;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link RelOptMaterializations#useMaterializedViews}.
 */
public class CustomMaterializedViewRecognitionRuleTest extends SqlToRelTestBase {

  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("mv0", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("empno", SqlTypeName.INTEGER)
            .add("ename", SqlTypeName.VARCHAR)
            .add("job", SqlTypeName.VARCHAR)
            .add("mgr", SqlTypeName.SMALLINT)
            .add("hiredate", SqlTypeName.DATE)
            .add("sal", SqlTypeName.DECIMAL)
            .add("comm", SqlTypeName.DECIMAL)
            .add("deptno", SqlTypeName.TINYINT)
            .build();
      }
    });
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null);
  }

  @Test void testCushionLikeOperatorRecognitionRule() {
    final RelBuilder relBuilder = RelBuilder.create(config().build());
    final RelNode query = relBuilder.scan("EMP")
        .filter(
            relBuilder.call(SqlStdOperatorTable.LIKE,
            relBuilder.field(1), relBuilder.literal("ABCD%")))
        .build();
    final RelNode target = relBuilder.scan("EMP")
        .filter(
            relBuilder.call(SqlStdOperatorTable.LIKE,
            relBuilder.field(1), relBuilder.literal("ABC%")))
        .build();
    final RelNode replacement = relBuilder.scan("mv0").build();
    final RelOptMaterialization relOptMaterialization =
        new RelOptMaterialization(replacement,
            target, null, Lists.newArrayList("mv0"));
    final List<UnifyRule> rules =
        new ArrayList<>(SubstitutionVisitor.DEFAULT_RULES);
    rules.add(CustomizedMaterializationRule.INSTANCE);
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized =
        RelOptMaterializations.useMaterializedViews(query,
            ImmutableList.of(relOptMaterialization), rules);
    final String optimized = ""
        + "LogicalCalc(expr#0..7=[{inputs}], expr#8=['ABCD%'], expr#9=[LIKE($t1, $t8)], proj#0."
        + ".7=[{exprs}], $condition=[$t9])\n"
        + "  LogicalProject(empno=[CAST($0):SMALLINT NOT NULL], ename=[CAST($1):VARCHAR(10)], "
        + "job=[CAST($2):VARCHAR(9)], mgr=[CAST($3):SMALLINT], hiredate=[CAST($4):DATE], "
        + "sal=[CAST($5):DECIMAL(7, 2)], comm=[CAST($6):DECIMAL(7, 2)], deptno=[CAST($7)"
        + ":TINYINT])\n"
        + "    LogicalTableScan(table=[[mv0]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(relOptimizedStr, isLinux(optimized));
  }

  /**
   * A customized materialization rule, which match expression of 'LIKE'
   * and match by compensation.
   */
  private static class CustomizedMaterializationRule
      extends SubstitutionVisitor.AbstractUnifyRule {

    public static final CustomizedMaterializationRule INSTANCE =
        new CustomizedMaterializationRule();

    private CustomizedMaterializationRule() {
      super(operand(MutableCalc.class, query(0)),
          operand(MutableCalc.class, target(0)), 1);
    }

    @Override protected SubstitutionVisitor.@Nullable UnifyResult apply(
        SubstitutionVisitor.UnifyRuleCall call) {
      final MutableCalc query = (MutableCalc) call.query;
      final Pair<RexNode, List<RexNode>> queryExplained = SubstitutionVisitor.explainCalc(query);
      final RexNode queryCond = queryExplained.left;
      final List<RexNode> queryProjs = queryExplained.right;

      final MutableCalc target = (MutableCalc) call.target;
      final Pair<RexNode, List<RexNode>> targetExplained =
          SubstitutionVisitor.explainCalc(target);
      final RexNode targetCond = targetExplained.left;
      final List<RexNode> targetProjs = targetExplained.right;
      final @Nullable Pair<RexNode, NlsString> parsedQ =
          parseLikeCondition(queryCond);
      final @Nullable Pair<RexNode, NlsString> parsedT =
          parseLikeCondition(targetCond);
      if (RexUtil.isIdentity(queryProjs, query.getInput().rowType)
          && RexUtil.isIdentity(targetProjs, target.getInput().rowType)
          && parsedQ != null && parsedT != null) {
        if (parsedQ.left.equals(parsedT.left)) {
          String literalQ = parsedQ.right.getValue();
          String literalT = parsedT.right.getValue();
          if (literalQ.endsWith("%") && literalT.endsWith("%")
              && !literalQ.equals(literalT)
              && literalQ.startsWith(literalT.substring(0, literalT.length() - 1))) {
            return call.result(MutableCalc.of(target, query.program));
          }
        }
      }
      return null;
    }

    private @Nullable Pair<RexNode, NlsString> parseLikeCondition(
        RexNode rexNode) {
      if (rexNode instanceof RexCall) {
        RexCall rexCall = (RexCall) rexNode;
        if (rexCall.getKind() == SqlKind.LIKE
            && rexCall.operands.get(0) instanceof RexInputRef
            && rexCall.operands.get(1) instanceof RexLiteral) {
          return Pair.of(rexCall.operands.get(0),
              (NlsString) ((RexLiteral) (rexCall.operands.get(1))).getValue());
        }
      }
      return null;
    }
  }

}
