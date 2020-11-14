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

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.materialize.CustomNormalizationRules.CustomizedNormalizationRule.Config;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateMergeRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.SqlToRelTestBase;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import org.junit.jupiter.api.Test;

public class CustomNormalizationRules extends SqlToRelTestBase {

  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("mv0", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("deptno", SqlTypeName.INTEGER)
            .add("count_sal", SqlTypeName.BIGINT)
            .build();
      }
    });
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null);
  }

  @Test void testCustomNormalizationRule() {
    final RelBuilder relBuilder = RelBuilder.create(config().build());
    final LogicalProject project = (LogicalProject) relBuilder.scan("EMP")
        .project(relBuilder.field("EMPNO"),
            relBuilder.field("ENAME"),
            relBuilder.field("JOB"),
            relBuilder.field("SAL"),
            relBuilder.field("DEPTNO")).build();
    final LogicalAggregate aggregate = (LogicalAggregate) relBuilder.push(project)
        .aggregate(
            relBuilder.groupKey(relBuilder.field(1, 0, "DEPTNO")),
            relBuilder.count(relBuilder.field(1, 0, "SAL")))
        .build();
    final ImmutableBitSet groupSet = ImmutableBitSet.of(4);
    final AggregateCall count = aggregate.getAggCallList().get(0);
    final AggregateCall call = AggregateCall.create(count.getAggregation(),
        count.isDistinct(), count.isApproximate(),
        count.ignoreNulls(), ImmutableList.of(3),
        count.filterArg, null, count.collation,
        count.getType(), count.getName());
    final RelNode query = LogicalAggregate.create(project, aggregate.getHints(),
        groupSet, ImmutableList.of(groupSet), ImmutableList.of(call));
    final RelNode target = aggregate;
    final RelNode replacement = relBuilder.scan("mv0").build();
    final RelOptMaterialization relOptMaterialization =
        new RelOptMaterialization(replacement,
            target, null, Lists.newArrayList("mv0"));
    final List<RelOptRule> optRules = new ArrayList<>();
    optRules.addAll(SubstitutionVisitor.NORMALIZATION_RULES);
    optRules.add(CustomizedNormalizationRule.Config.DEFAULT.toRule());
    final List<Pair<RelNode, List<RelOptMaterialization>>> relOptimized =
        RelOptMaterializations.useMaterializedViews(query,
            ImmutableList.of(relOptMaterialization), ImmutableList.of());

    final String optimized = ""
        + "LogicalProject(deptno=[CAST($0):TINYINT], count_sal=[$1])\n"
        + "  LogicalTableScan(table=[[mv0]])\n";
    final String relOptimizedStr = RelOptUtil.toString(relOptimized.get(0).getKey());
    assertThat(relOptimizedStr, isLinux(optimized));
  }

  /**
   * A customized normalization rule, which compensate project on Agggreate.
   */
  public static class CustomizedNormalizationRule extends
      RelRule<Config> implements TransformationRule {

    public boolean visited = false;

    public CustomizedNormalizationRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Aggregate aggregate = call.rel(0);
      RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      RelDataType rowType = aggregate.getRowType();
      List<String> fieldNames = aggregate.getRowType().getFieldNames();
      List<RexNode> projs = aggregate.getCluster().getRexBuilder()
          .identityProjects(rowType);
      RexProgramBuilder rexProgram = new RexProgramBuilder(rowType, rexBuilder);
      for (int i = 0; i < projs.size(); i++) {
        rexProgram.addProject(projs.get(i), fieldNames.get(i));
      }
      LogicalCalc project = LogicalCalc
          .create(aggregate, rexProgram.getProgram());
      visited = true;
      call.transformTo(project);
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
      CustomizedNormalizationRule.Config DEFAULT = EMPTY
          .withOperandSupplier(b ->
              b.operand(LogicalAggregate.class).anyInputs())
          .as(CustomizedNormalizationRule.Config.class);
      @Override default CustomizedNormalizationRule toRule() {
        return new CustomizedNormalizationRule(this);
      }
    }
  }

}
