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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that recognizes  a {@link org.apache.calcite.rel.core.Aggregate}
 * on top of a {@link org.apache.calcite.rel.core.Project} where the aggregate's group set
 * contains literals (true, false, DATE, chars, etc), and removes the literals from the
 * group keys by joining with a dummy table of literals.
 *
 * <pre>{@code
 * select avg(sal)
 * from emp
 * group by true, DATE '2022-01-01';
 * }</pre>
 * becomes
 * <pre>{@code
 * select avg(sal)
 * from emp, (select true x, DATE '2022-01-01' d) dummy
 * group by dummy.x, dummy.d;
 * }</pre>
 */
@Value.Enclosing
public final class AggregateProjectConstantToDummyJoinRule
    extends RelRule<AggregateProjectConstantToDummyJoinRule.Config> {

  /** Creates an AggregateProjectConstantToDummyJoinRule. */
  private AggregateProjectConstantToDummyJoinRule(Config config) {
    super(config);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    for (int groupKey : aggregate.getGroupSet().asList()) {
      if (groupKey >= aggregate.getRowType().getFieldCount()) {
        continue;
      }
      RexNode groupKeyProject = project.getProjects().get(groupKey);
      if (groupKeyProject instanceof RexLiteral) {
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
    int offset = project.getInput().getRowType().getFieldCount();

    RelDataTypeFactory.Builder valuesType = rexBuilder.getTypeFactory().builder();
    List<RexLiteral> literals = new ArrayList<>();
    List<RexNode> projects = project.getProjects();
    for (int i = 0; i < projects.size(); i++) {
      RexNode node = projects.get(i);
      if (node instanceof RexLiteral) {
        literals.add((RexLiteral) node);
        valuesType.add(project.getRowType().getFieldList().get(i));
      }
    }
    builder.values(ImmutableList.of(literals), valuesType.build());

    builder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));

    List<RexNode> newProjects = new ArrayList<>();
    int literalCounter = 0;
    for (RexNode exp : project.getProjects()) {
      if (exp instanceof RexLiteral) {
        newProjects.add(builder.field(offset + literalCounter++));
      } else {
        newProjects.add(exp);
      }
    }

    builder.project(newProjects);
    builder.aggregate(
        builder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets()),
        aggregate.getAggCallList());

    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateProjectConstantToDummyJoinRule.Config.of()
        .withOperandFor(Aggregate.class, Project.class);

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
