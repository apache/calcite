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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that creates a {@code SemiJoin} from a
 * {@link org.apache.calcite.rel.core.Join} on top of a
 * {@link org.apache.calcite.rel.logical.LogicalAggregate} or
 * on a {@link org.apache.calcite.rel.RelNode} which is
 * unique for join's right keys.
 */
public abstract class SemiJoinRule
    extends RelRule<SemiJoinRule.Config>
    implements TransformationRule {
  private static boolean isJoinTypeSupported(Join join) {
    final JoinRelType type = join.getJoinType();
    return type == JoinRelType.INNER || type == JoinRelType.LEFT;
  }

  /**
   * Tests if an Aggregate always produces 1 row and 0 columns.
   */
  private static boolean isEmptyAggregate(Aggregate aggregate) {
    return aggregate.getRowType().getFieldCount() == 0;
  }

  /** Creates a SemiJoinRule. */
  protected SemiJoinRule(Config config) {
    super(config);
  }

  protected void perform(RelOptRuleCall call, @Nullable Project project,
      Join join, RelNode left, Aggregate aggregate) {
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (project != null) {
      final ImmutableBitSet bits =
          RelOptUtil.InputFinder.bits(project.getProjects(), null);
      final ImmutableBitSet rightBits =
          ImmutableBitSet.range(left.getRowType().getFieldCount(),
              join.getRowType().getFieldCount());
      if (bits.intersects(rightBits)) {
        return;
      }
    } else {
      if (join.getJoinType().projectsRight()
          && !isEmptyAggregate(aggregate)) {
        return;
      }
    }
    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(
        ImmutableBitSet.range(aggregate.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }
    if (!joinInfo.isEqui()) {
      return;
    }
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(left);
    switch (join.getJoinType()) {
    case INNER:
      final List<Integer> newRightKeyBuilder = new ArrayList<>();
      final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
      for (int key : joinInfo.rightKeys) {
        newRightKeyBuilder.add(aggregateKeys.get(key));
      }
      final ImmutableIntList newRightKeys = ImmutableIntList.copyOf(newRightKeyBuilder);
      relBuilder.push(aggregate.getInput());
      final RexNode newCondition =
          RelOptUtil.createEquiJoinCondition(relBuilder.peek(2, 0),
              joinInfo.leftKeys, relBuilder.peek(2, 1), newRightKeys,
              rexBuilder);
      relBuilder.semiJoin(newCondition).hints(join.getHints());
      break;

    case LEFT:
      // The right-hand side produces no more than 1 row (because of the
      // Aggregate) and no fewer than 1 row (because of LEFT), and therefore
      // we can eliminate the semi-join.
      break;

    default:
      throw new AssertionError(join.getJoinType());
    }
    if (project != null) {
      relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());
    }
    final RelNode relNode = relBuilder.build();
    call.transformTo(relNode);
  }

  /** SemiJoinRule that matches a Project on top of a Join with an Aggregate
   * as its right child.
   *
   * @see CoreRules#PROJECT_TO_SEMI_JOIN */
  public static class ProjectToSemiJoinRule extends SemiJoinRule {
    /** Creates a ProjectToSemiJoinRule. */
    protected ProjectToSemiJoinRule(ProjectToSemiJoinRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public ProjectToSemiJoinRule(Class<Project> projectClass,
        Class<Join> joinClass, Class<Aggregate> aggregateClass,
        RelBuilderFactory relBuilderFactory, String description) {
      this(ProjectToSemiJoinRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .withDescription(description)
          .as(ProjectToSemiJoinRuleConfig.class)
          .withOperandFor(projectClass, joinClass, aggregateClass));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final Join join = call.rel(1);
      final RelNode left = call.rel(2);
      final Aggregate aggregate = call.rel(3);
      perform(call, project, join, left, aggregate);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface ProjectToSemiJoinRuleConfig extends SemiJoinRule.Config {
      ProjectToSemiJoinRuleConfig DEFAULT = ImmutableProjectToSemiJoinRuleConfig.of()
          .withDescription("SemiJoinRule:project")
          .withOperandFor(Project.class, Join.class, Aggregate.class);

      @Override default ProjectToSemiJoinRule toRule() {
        return new ProjectToSemiJoinRule(this);
      }

      /** Defines an operand tree for the given classes. */
      default ProjectToSemiJoinRuleConfig withOperandFor(Class<? extends Project> projectClass,
          Class<? extends Join> joinClass,
          Class<? extends Aggregate> aggregateClass) {
        return withOperandSupplier(b ->
            b.operand(projectClass).oneInput(b2 ->
                b2.operand(joinClass)
                    .predicate(SemiJoinRule::isJoinTypeSupported).inputs(
                        b3 -> b3.operand(RelNode.class).anyInputs(),
                        b4 -> b4.operand(aggregateClass).anyInputs())))
            .as(ProjectToSemiJoinRuleConfig.class);
      }
    }
  }

  /** SemiJoinRule that matches a Join with an empty Aggregate as its right
   * input.
   *
   * @see CoreRules#JOIN_TO_SEMI_JOIN */
  public static class JoinToSemiJoinRule extends SemiJoinRule {
    /** Creates a JoinToSemiJoinRule. */
    protected JoinToSemiJoinRule(JoinToSemiJoinRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public JoinToSemiJoinRule(
        Class<Join> joinClass, Class<Aggregate> aggregateClass,
        RelBuilderFactory relBuilderFactory, String description) {
      this(JoinToSemiJoinRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
          .withDescription(description)
          .as(JoinToSemiJoinRuleConfig.class)
          .withOperandFor(joinClass, aggregateClass));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final RelNode left = call.rel(1);
      final Aggregate aggregate = call.rel(2);
      perform(call, null, join, left, aggregate);
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface JoinToSemiJoinRuleConfig extends SemiJoinRule.Config {
      JoinToSemiJoinRuleConfig DEFAULT = ImmutableJoinToSemiJoinRuleConfig.of()
          .withDescription("SemiJoinRule:join")
          .withOperandFor(Join.class, Aggregate.class);

      @Override default JoinToSemiJoinRule toRule() {
        return new JoinToSemiJoinRule(this);
      }

      /** Defines an operand tree for the given classes. */
      default JoinToSemiJoinRuleConfig withOperandFor(Class<Join> joinClass,
          Class<Aggregate> aggregateClass) {
        return withOperandSupplier(b ->
            b.operand(joinClass).predicate(SemiJoinRule::isJoinTypeSupported).inputs(
                b2 -> b2.operand(RelNode.class).anyInputs(),
                b3 -> b3.operand(aggregateClass).anyInputs()))
            .as(JoinToSemiJoinRuleConfig.class);
      }
    }
  }

  /**
   * SemiJoinRule that matches a Project on top of a Join with a RelNode
   * which is unique for Join's right keys.
   *
   * @see CoreRules#JOIN_ON_UNIQUE_TO_SEMI_JOIN */
  public static class JoinOnUniqueToSemiJoinRule extends SemiJoinRule {

    /** Creates a JoinOnUniqueToSemiJoinRule. */
    protected JoinOnUniqueToSemiJoinRule(JoinOnUniqueToSemiJoinRuleConfig config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final Join join = call.rel(1);
      final RelNode left = call.rel(2);

      final ImmutableBitSet bits =
          RelOptUtil.InputFinder.bits(project.getProjects(), null);
      final ImmutableBitSet rightBits =
          ImmutableBitSet.range(left.getRowType().getFieldCount(),
              join.getRowType().getFieldCount());
      return !bits.intersects(rightBits);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final Join join = call.rel(1);
      final RelNode left = call.rel(2);
      final RelNode right = call.rel(3);

      final JoinInfo joinInfo = join.analyzeCondition();
      final RelOptCluster cluster = join.getCluster();
      final RelMetadataQuery mq = cluster.getMetadataQuery();
      final Boolean unique = mq.areColumnsUnique(right, joinInfo.rightSet());
      if (unique != null && unique) {
        final RelBuilder builder = call.builder();
        switch (join.getJoinType()) {
        case INNER:
          builder.push(left);
          builder.push(right);
          builder.join(JoinRelType.SEMI, join.getCondition());
          break;
        case LEFT:
          builder.push(left);
          break;
        default:
          throw new AssertionError(join.getJoinType());
        }
        builder.project(project.getProjects());
        call.transformTo(builder.build());
      }
    }

    /**
     * Rule configuration.
     */
    @Value.Immutable
    public interface JoinOnUniqueToSemiJoinRuleConfig extends SemiJoinRule.Config {
      JoinOnUniqueToSemiJoinRuleConfig DEFAULT = ImmutableJoinOnUniqueToSemiJoinRuleConfig.of()
          .withDescription("SemiJoinRule:unique")
          .withOperandSupplier(b ->
              b.operand(Project.class).oneInput(
                  b2 -> b2.operand(Join.class).predicate(SemiJoinRule::isJoinTypeSupported).inputs(
                      b3 -> b3.operand(RelNode.class).anyInputs(),
                      b4 -> b4.operand(RelNode.class)
                          // If RHS is Aggregate, it will be covered by ProjectToSemiJoinRule
                          .predicate(n -> !(n instanceof Aggregate))
                          .anyInputs()
                  )))
          .as(JoinOnUniqueToSemiJoinRuleConfig.class);

      @Override default JoinOnUniqueToSemiJoinRule toRule() {
        return new JoinOnUniqueToSemiJoinRule(this);
      }
    }
  }

  /**
   * Rule configuration.
   */
  public interface Config extends RelRule.Config {
    @Override SemiJoinRule toRule();
  }
}
