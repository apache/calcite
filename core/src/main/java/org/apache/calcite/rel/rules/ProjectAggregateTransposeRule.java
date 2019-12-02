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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Aggregate}.
 */
public class ProjectAggregateTransposeRule extends RelOptRule {

  public static final ProjectAggregateTransposeRule INSTANCE =
      new ProjectAggregateTransposeRule(Project.class,
          Aggregate.class, RelFactories.LOGICAL_BUILDER);

  /**
   * Creates a ProjectAggregateTransposeRule.
   */
  private ProjectAggregateTransposeRule(Class<? extends Project> project,
      Class<? extends Aggregate> aggregate, RelBuilderFactory relBuilderFactory) {
    super(operand(project, operand(aggregate, any())),
        relBuilderFactory, "ProjectAggregateTransposeRule");
  }

  public void onMatch(RelOptRuleCall call) {
    final LogicalProject proRel = call.rel(0);
    final LogicalAggregate aggRel = call.rel(1);

    final Mappings.TargetMapping mapping =
        Project.getMapping(aggRel.getRowType().getFieldCount(), proRel.getProjects());
    if (mapping == null || Mappings.keepsOrdering(mapping)) {
      // do nothing
      return;
    }
    // Project              Aggregate
    //    Aggregate  --->      Project
    final List<Pair<Integer, Integer>> pairs = new ArrayList<>();
    final List<Integer> groupings = aggRel.getGroupSet().toList();
    RelBuilder relBuilder = call.builder();
    final List<Integer> posList = new ArrayList<>();

    for (int i = 0; i < groupings.size(); i++) {
      pairs.add(Pair.of(mapping.getTarget(groupings.get(i)), i));
    }
    Collections.sort(pairs);
    pairs.forEach(pair -> posList.add(pair.right));

    for (int i = posList.size(); i < aggRel.getInput().getRowType().getFieldCount(); i++) {
      posList.add(i);
    }
    List<RexNode> newProj = posList.stream().map(
        pos -> RexInputRef.of(pos, aggRel.getInput().getRowType()))
        .collect(Collectors.toList());
    final RelNode project = relBuilder.push(aggRel.getInput()).project(newProj).build();

    RelNode newAgg = relBuilder.push(project).aggregate(relBuilder.groupKey(aggRel.getGroupSet()),
        aggRel.getAggCallList()).build();

    if (!rowTypesAreEquivalent(proRel, newAgg)) {
      return;
    }

    call.transformTo(newAgg);
  }

  private boolean rowTypesAreEquivalent(
      RelNode rel0, RelNode rel1) {
    if (rel0.getRowType().getFieldCount() != rel1.getRowType().getFieldCount()) {
      return false;
    }
    for (Pair<RelDataTypeField, RelDataTypeField> pair
        : Pair.zip(rel0.getRowType().getFieldList(), rel1.getRowType().getFieldList())) {
      if (!pair.left.getType().equals(pair.right.getType())) {
        return false;
      }
    }
    return true;
  }
}
// End ProjectAggregateTransposeRule.java
