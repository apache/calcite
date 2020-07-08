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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that matches an {@link org.apache.calcite.rel.core.Project}
 * on a {@link org.apache.calcite.rel.core.Join} and removes the left input
 * of the join provided that the left input is also a left join if possible.
 *
 * <p>For instance,</p>
 *
 * <blockquote>
 * <pre>select s.product_id, pc.product_id
 * from sales as s
 * left join product as p
 *   on s.product_id = p.product_id
 * left join product_class pc
 *   on s.product_id = pc.product_id</pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 * <pre>select s.product_id, pc.product_id
 * from sales as s
 * left join product_class pc
 *   on s.product_id = pc.product_id</pre></blockquote>
 *
 * @see CoreRules#PROJECT_JOIN_JOIN_REMOVE
 */
public class ProjectJoinJoinRemoveRule
    extends RelRule<ProjectJoinJoinRemoveRule.Config>
    implements SubstitutionRule {
  /** @deprecated Use {@link CoreRules#PROJECT_JOIN_JOIN_REMOVE}. */
  @Deprecated // to be removed before 1.25
  public static final ProjectJoinJoinRemoveRule INSTANCE =
      Config.DEFAULT.toRule();

  /** Creates a ProjectJoinJoinRemoveRule. */
  protected ProjectJoinJoinRemoveRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectJoinJoinRemoveRule(
      Class<? extends Project> projectClass,
      Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(projectClass, joinClass));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Join topJoin = call.rel(1);
    final Join bottomJoin = call.rel(2);
    int leftBottomChildSize = bottomJoin.getLeft().getRowType().getFieldCount();

    // Check whether the project uses columns in the right input of bottom join.
    for (RexNode expr: project.getProjects()) {
      if (RelOptUtil.InputFinder.bits(expr).asList().stream().anyMatch(
          i -> i >= leftBottomChildSize
              && i < bottomJoin.getRowType().getFieldCount())) {
        return;
      }
    }

    // Check whether the top join uses columns in the right input of bottom join.
    final List<Integer> leftKeys = new ArrayList<>();
    RelOptUtil.splitJoinCondition(topJoin.getLeft(), topJoin.getRight(),
        topJoin.getCondition(), leftKeys, new ArrayList<>(),
        new ArrayList<>());
    if (leftKeys.stream().anyMatch(s -> s >= leftBottomChildSize)) {
      return;
    }

    // Check whether left join keys in top join and bottom join are equal.
    final List<Integer> leftChildKeys = new ArrayList<>();
    final List<Integer> rightChildKeys = new ArrayList<>();
    RelOptUtil.splitJoinCondition(bottomJoin.getLeft(), bottomJoin.getRight(),
        bottomJoin.getCondition(), leftChildKeys, rightChildKeys,
        new ArrayList<>());
    if (!leftKeys.equals(leftChildKeys)) {
      return;
    }

    // Make sure that right keys of bottom join are unique.
    final ImmutableBitSet.Builder columns = ImmutableBitSet.builder();
    rightChildKeys.forEach(columns::set);
    final RelMetadataQuery mq = call.getMetadataQuery();
    if (!mq.areColumnsUnique(bottomJoin.getRight(), columns.build())) {
      return;
    }

    int offset = bottomJoin.getRight().getRowType().getFieldCount();
    final RelBuilder relBuilder = call.builder();

    final RexNode condition = RexUtil.shift(topJoin.getCondition(),
        leftBottomChildSize, -offset);
    final RelNode join = relBuilder.push(bottomJoin.getLeft())
        .push(topJoin.getRight())
        .join(topJoin.getJoinType(), condition)
        .build();

    final List<RexNode> newExprs = project.getProjects().stream()
        .map(expr -> RexUtil.shift(expr, leftBottomChildSize, -offset))
        .collect(Collectors.toList());
    relBuilder.push(join).project(newExprs);
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(LogicalProject.class, LogicalJoin.class);

    @Override default ProjectJoinJoinRemoveRule toRule() {
      return new ProjectJoinJoinRemoveRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
        Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
          b0.operand(projectClass).oneInput(b1 ->
              b1.operand(joinClass).predicate(j ->
                  j.getJoinType() == JoinRelType.LEFT)
              .inputs(b2 ->
                  b2.operand(joinClass).predicate(j ->
                      j.getJoinType() == JoinRelType.LEFT)
                  .anyInputs())))
          .as(Config.class);
    }
  }
}
