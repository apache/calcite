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
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that matches an {@link Project}
 * on a {@link Join} and removes the join provided that the join is a left join
 * or right join and the join keys are unique.
 *
 * <p>For instance,
 *
 * <blockquote>
 * <pre>select s.product_id
 * from sales as s
 * left join product as p
 * on s.product_id = p.product_id</pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 * <pre>select s.product_id from sales as s</pre></blockquote>
 */
public class ProjectJoinRemoveRule
    extends RelRule<ProjectJoinRemoveRule.Config>
    implements SubstitutionRule {

  /** Creates a ProjectJoinRemoveRule. */
  protected ProjectJoinRemoveRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectJoinRemoveRule(
      Class<? extends Project> projectClass,
      Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(projectClass, joinClass));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Join join = call.rel(1);
    final boolean isLeftJoin = join.getJoinType() == JoinRelType.LEFT;
    int lower = isLeftJoin
        ? join.getLeft().getRowType().getFieldCount() - 1 : 0;
    int upper = isLeftJoin
        ? join.getRowType().getFieldCount()
        : join.getLeft().getRowType().getFieldCount();

    // Check whether the project uses columns whose index is between
    // lower(included) and upper(excluded).
    for (RexNode expr: project.getProjects()) {
      if (RelOptUtil.InputFinder.bits(expr).asList().stream().anyMatch(
          i -> i >= lower && i < upper)) {
        return;
      }
    }

    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();
    RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
        join.getCondition(), leftKeys, rightKeys,
        new ArrayList<>());

    final List<Integer> joinKeys = isLeftJoin ? rightKeys : leftKeys;
    final ImmutableBitSet.Builder columns = ImmutableBitSet.builder();
    joinKeys.forEach(columns::set);

    final RelMetadataQuery mq = call.getMetadataQuery();
    if (!mq.areColumnsUnique(isLeftJoin ? join.getRight() : join.getLeft(),
        columns.build())) {
      return;
    }

    RelNode node;
    if (isLeftJoin) {
      node = project
          .copy(project.getTraitSet(), join.getLeft(), project.getProjects(),
              project.getRowType());
    } else {
      final int offset = join.getLeft().getRowType().getFieldCount();
      final List<RexNode> newExprs = project.getProjects().stream()
          .map(expr -> RexUtil.shift(expr, -offset))
          .collect(Collectors.toList());
      node = project.copy(project.getTraitSet(), join.getRight(), newExprs,
          project.getRowType());
    }
    call.transformTo(node);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(LogicalProject.class, LogicalJoin.class);

    @Override default ProjectJoinRemoveRule toRule() {
      return new ProjectJoinRemoveRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
        Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
          b0.operand(projectClass).oneInput(b1 ->
              b1.operand(joinClass).predicate(join ->
                  join.getJoinType() == JoinRelType.LEFT
                      || join.getJoinType() == JoinRelType.RIGHT).anyInputs()))
          .as(Config.class);
    }
  }
}
