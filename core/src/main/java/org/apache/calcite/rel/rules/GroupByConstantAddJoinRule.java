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
import org.apache.calcite.tools.RelBuilderFactory;
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
public class GroupByConstantAddJoinRule
    extends RelRule<GroupByConstantAddJoinRule.Config> {

  /**
   * Creates a RelRule.
   *
   */
  protected GroupByConstantAddJoinRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public GroupByConstantAddJoinRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory) {
    this(CoreRules.GROUP_BY_CONSTANT_ADD_JOIN_RULE.config
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(aggregateClass, projectClass));
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    // avoids transforming correlated queries
    if (aggregate.getAggCallList().size() == 0) {
      return false;
    }

    boolean hasBooleanLiteral = false;

    for (int groupKey: aggregate.getGroupSet().asList()) {
      if (groupKey >= aggregate.getRowType().getFieldCount()) {
        continue;
      }
      hasBooleanLiteral = (aggregate.getRowType().getFieldList().get(groupKey).getType().getFamily()
          == SqlTypeFamily.BOOLEAN)
          && (project.getProjects().get(groupKey) instanceof RexLiteral);

      if (hasBooleanLiteral) {
        return true;
      }
    }

    return hasBooleanLiteral;
  }

  private static LogicalValues getValues(
      Aggregate aggregate, Project project, RexBuilder rexBuilder, RelBuilder builder) {
    final ImmutableList.Builder<ImmutableList<RexLiteral>> tuples =
        ImmutableList.builder();
    ImmutableList.Builder<RexLiteral> b = ImmutableList.builder();
    boolean truePresent = false;
    boolean falsePresent = false;

    for (int groupKey: aggregate.getGroupSet().asList()) {
      if (project.getProjects().get(groupKey) instanceof RexLiteral) {
        if (project.getProjects().get(groupKey).isAlwaysTrue()) {
          truePresent = true;
        }
        if (project.getProjects().get(groupKey).isAlwaysFalse()) {
          falsePresent = true;
        }
      }
    }

    RelDataTypeFactory.Builder relDataTypeBuilder = rexBuilder.getTypeFactory().builder();
    if (truePresent) {
      b.add(rexBuilder.makeLiteral(true));
      relDataTypeBuilder.add("T", SqlTypeName.BOOLEAN);
    }
    if (falsePresent) {
      b.add(rexBuilder.makeLiteral(false));
      relDataTypeBuilder.add("F", SqlTypeName.BOOLEAN);
    }
    tuples.add(b.build());
    LogicalValues values = LogicalValues.create(builder.getCluster(), relDataTypeBuilder.build(),
        tuples.build());

    return values;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    RelBuilder builder = call.builder();
    RexBuilder rexBuilder = builder.getRexBuilder();

    builder.push(project.getInput());
    LogicalValues values = getValues(aggregate, project, rexBuilder, builder);
    builder.push(values);

    builder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));
    Join join = (Join) builder.build();

    List<RexNode> newProjects = new ArrayList<>();
    List<String> names = new ArrayList<>();

    int falseIndex = join.getRowType().getFieldCount() - 1;
    int trueIndex = join.getRight().getRowType().getFieldCount() == 1
        ? falseIndex : falseIndex - 1;
    for (Pair<RexNode, String> pair: project.getNamedProjects()) {
      if (pair.getKey() instanceof RexLiteral) {
        if (pair.getKey().isAlwaysTrue()) {
          newProjects.add(RexInputRef.of(trueIndex, join.getRowType()));
          names.add(join.getRowType().getFieldList().get(trueIndex).getName());
        } else if (pair.getKey().isAlwaysFalse()) {
          newProjects.add(RexInputRef.of(falseIndex, join.getRowType()));
          names.add(join.getRowType().getFieldList().get(falseIndex).getName());
        }
      } else {
        newProjects.add(pair.getKey());
        names.add(pair.getValue());
      }
    }

    Project newProject = LogicalProject.create(join, project.getHints(), newProjects, names);
    Aggregate newAggregate = aggregate.copy(aggregate.getTraitSet(), newProject,
        aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
    call.transformTo(newAggregate);
  }

  /**
   * Rule Configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableGroupByConstantAddJoinRule.Config.of()
        .withOperandFor(LogicalAggregate.class, LogicalProject.class);

    @Override default GroupByConstantAddJoinRule toRule() {
      return new GroupByConstantAddJoinRule(this);
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
