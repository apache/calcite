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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that recognizes  a {@link org.apache.calcite.rel.core.Aggregate}
 * on top of a {@link org.apache.calcite.rel.core.Project} where the aggregate's group set
 * contains boolean literals (true, false), and removes the literals from the group keys by joining
 * with a dummy table of boolean literals.
 *
 * select avg(sal)
 * from emp
 * group by true;
 *
 * becomes
 *
 * select avg(sal)
 * from emp, (select true x) dummy
 * group by dummy.x;
 */
@Value.Enclosing
public final class AggregateProjectConstantToDummyJoinRule
    extends RelRule<AggregateProjectConstantToDummyJoinRule.Config> {

  /**
   * Creates a RelRule.
   *
   */
  private AggregateProjectConstantToDummyJoinRule(Config config) {
    super(config);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    // avoids transforming correlated queries
    if (aggregate.getAggCallList().size() == 0) {
      return false;
    }

    for (int groupKey: aggregate.getGroupSet().asList()) {
      if (groupKey >= aggregate.getRowType().getFieldCount()) {
        continue;
      }
      RexNode groupKeyProject = project.getProjects().get(groupKey);
      if (groupKeyProject instanceof RexLiteral
          && groupKeyProject.getType().getFamily() == SqlTypeFamily.BOOLEAN) {
        return true;
      }
    }

    return false;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    RelBuilder builder = call.builder();
    RexBuilder rexBuilder = builder.getRexBuilder();

    builder.push(project.getInput());
    builder.values(new String[]{"T", "F"}, true, false);
    builder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));

    List<RexNode> newProjects = new ArrayList<>();

    for (Pair<RexNode, String> pair: project.getNamedProjects()) {
      if (pair.getKey() instanceof RexLiteral) {
        if (pair.getKey().isAlwaysTrue()) {
          newProjects.add(builder.field("T"));
        } else if (pair.getKey().isAlwaysFalse()) {
          newProjects.add(builder.field("F"));
        }
      } else {
        newProjects.add(pair.getKey());
      }
    }

    builder.project(newProjects);
    builder.aggregate(builder.groupKey(
        aggregate.getGroupSet(),(Iterable<ImmutableBitSet>) aggregate.getGroupSets()),
        aggregate.getAggCallList());

    call.transformTo(builder.build());
  }

  /**
   * Rule Configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateProjectConstantToDummyJoinRule.Config.of()
        .withOperandFor(LogicalAggregate.class, LogicalProject.class);

    @Override default AggregateProjectConstantToDummyJoinRule toRule() {
      return new AggregateProjectConstantToDummyJoinRule(this);
    }

    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Project> projectClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass).oneInput(b1 ->
              b1.operand(projectClass).anyInputs()))
          .as(Config.class);
    }
  }
}
