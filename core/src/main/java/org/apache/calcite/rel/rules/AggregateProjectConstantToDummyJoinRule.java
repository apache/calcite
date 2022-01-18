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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that recognizes  a {@link org.apache.calcite.rel.core.Aggregate}
 * on top of a {@link org.apache.calcite.rel.core.Project} where the aggregate's group set
 * contains boolean literals (true, false), and removes the literals from the group keys by joining
 * with a dummy table of boolean literals.
 *
 * <pre>{@code
 * select avg(sal)
 * from emp
 * group by true;
 * }</pre>
 * becomes
 * <pre>{@code
 * select avg(sal)
 * from emp, (select true x) dummy
 * group by dummy.x;
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

    for (int groupKey: aggregate.getGroupSet().asList()) {
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

    List<String> fieldNames = new ArrayList<>();
    List<Object> valueObjects = new ArrayList<>();
    int literalCounter = 0;
    String literalPrefix = "LTRL";
    for (RexNode node: project.getProjects()) {
      if (node instanceof RexLiteral) {
        String fieldName = literalPrefix + literalCounter++;
        fieldNames.add(fieldName);
        valueObjects.add(node);
      }
    }

    builder.values(fieldNames.toArray(new String[0]), valueObjects.toArray());
    builder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));

    List<RexNode> newProjects = new ArrayList<>();

    literalCounter = 0;
    for (RexNode exp : project.getProjects()) {
      if (exp instanceof RexLiteral) {
        newProjects.add(builder.field(literalPrefix + literalCounter++));
      } else {
        newProjects.add(exp);
      }
    }

    builder.project(newProjects);
    builder.aggregate(
        builder.groupKey(
            aggregate.getGroupSet(), (Iterable<ImmutableBitSet>) aggregate.getGroupSets()),
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
