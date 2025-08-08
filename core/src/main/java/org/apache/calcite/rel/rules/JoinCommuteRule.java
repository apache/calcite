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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.List;

/**
 * Planner rule that permutes the inputs to a
 * {@link org.apache.calcite.rel.core.Join}.
 *
 * <p>Permutation of outer joins can be turned on/off by specifying the
 * swapOuter flag in the constructor.
 *
 * <p>To preserve the order of columns in the output row, the rule adds a
 * {@link org.apache.calcite.rel.core.Project}.
 *
 * @see CoreRules#JOIN_COMMUTE
 * @see CoreRules#JOIN_COMMUTE_OUTER
 */
@Value.Enclosing
public class JoinCommuteRule
    extends RelRule<JoinCommuteRule.Config>
    implements TransformationRule {

  /** Creates a JoinCommuteRule. */
  protected JoinCommuteRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public JoinCommuteRule(Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory, boolean swapOuter) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(clazz)
        .withSwapOuter(swapOuter));
  }

  @Deprecated // to be removed before 2.0
  public JoinCommuteRule(Class<? extends Join> clazz,
      ProjectFactory projectFactory) {
    this(clazz, RelBuilder.proto(Contexts.of(projectFactory)), false);
  }

  @Deprecated // to be removed before 2.0
  public JoinCommuteRule(Class<? extends Join> clazz,
      ProjectFactory projectFactory, boolean swapOuter) {
    this(clazz, RelBuilder.proto(Contexts.of(projectFactory)), swapOuter);
  }

  //~ Methods ----------------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public static @Nullable RelNode swap(Join join) {
    return swap(join, false,
        RelFactories.LOGICAL_BUILDER.create(join.getCluster(), null));
  }

  @Deprecated // to be removed before 2.0
  public static @Nullable RelNode swap(Join join, boolean swapOuterJoins) {
    return swap(join, swapOuterJoins,
        RelFactories.LOGICAL_BUILDER.create(join.getCluster(), null));
  }

  /**
   * Returns a relational expression with the inputs switched round. Does not
   * modify <code>join</code>. Returns null if the join cannot be swapped (for
   * example, because it is an outer join).
   *
   * @param join              join to be swapped
   * @param swapOuterJoins    whether outer joins should be swapped
   * @param relBuilder        Builder for relational expressions
   * @return swapped join if swapping possible; else null
   */
  public static @Nullable RelNode swap(Join join, boolean swapOuterJoins,
      RelBuilder relBuilder) {
    final JoinRelType joinType = join.getJoinType();
    if (!swapOuterJoins && joinType != JoinRelType.INNER) {
      return null;
    }
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelDataType leftRowType = join.getLeft().getRowType();
    final RelDataType rightRowType = join.getRight().getRowType();
    final VariableReplacer variableReplacer =
        new VariableReplacer(rexBuilder, leftRowType, rightRowType);
    final RexNode oldCondition = join.getCondition();
    RexNode condition = variableReplacer.apply(oldCondition);

    // NOTE jvs 14-Mar-2006: We preserve attribute semiJoinDone after the
    // swap.  This way, we will generate one semijoin for the original
    // join, and one for the swapped join, and no more.  This
    // doesn't prevent us from seeing any new combinations assuming
    // that the planner tries the desired order (semi-joins after swaps).
    Join newJoin =
        join.copy(join.getTraitSet(), condition, join.getRight(),
            join.getLeft(), joinType.swap(), join.isSemiJoinDone());
    final List<RexNode> exps =
        RelOptUtil.createSwappedJoinExprs(newJoin, join, true);
    return relBuilder.push(newJoin)
        .project(exps, join.getRowType().getFieldNames())
        .build();
  }

  public static RexNode swapJoinCond(RexNode cond, Join join, RexBuilder rexBuilder) {
    final VariableReplacer variableReplacer =
        new VariableReplacer(rexBuilder, join.getLeft().getRowType(), join.getRight().getRowType());
    return variableReplacer.apply(cond);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    // SEMI and ANTI join cannot be swapped.
    if (!join.getJoinType().projectsRight()) {
      return false;
    }

    // If rightToLeftOnly is enabled, only allow RIGHT joins to be swapped
    if (config.isRightToLeftOnly() && join.getJoinType() != JoinRelType.RIGHT) {
      return false;
    }

    // Suppress join with "true" condition (that is, cartesian joins).
    return config.isAllowAlwaysTrueCondition()
        || !join.getCondition().isAlwaysTrue();
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    Join join = call.rel(0);

    final RelNode swapped = swap(join, config.isSwapOuter(), call.builder());
    if (swapped == null) {
      return;
    }

    // The result is either a Project or, if the project is trivial, a
    // raw Join.
    final Join newJoin =
        swapped instanceof Join
            ? (Join) swapped
            : (Join) swapped.getInput(0);

    call.transformTo(swapped);

    // We have converted join='a join b' into swapped='select
    // a0,a1,a2,b0,b1 from b join a'. Now register that project='select
    // b0,b1,a0,a1,a2 from (select a0,a1,a2,b0,b1 from b join a)' is the
    // same as 'b join a'. If we didn't do this, the swap join rule
    // would fire on the new join, ad infinitum.
    final RelBuilder relBuilder = call.builder();
    final List<RexNode> exps =
        RelOptUtil.createSwappedJoinExprs(newJoin, join, false);
    relBuilder.push(swapped)
        .project(exps, newJoin.getRowType().getFieldNames());

    call.getPlanner().ensureRegistered(relBuilder.build(), newJoin);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Walks over an expression, replacing references to fields of the left and
   * right inputs.
   *
   * <p>If the field index is less than leftFieldCount, it must be from the
   * left, and so has rightFieldCount added to it; if the field index is
   * greater than leftFieldCount, it must be from the right, so we subtract
   * leftFieldCount from it.
   */
  private static class VariableReplacer extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final List<RelDataTypeField> leftFields;
    private final List<RelDataTypeField> rightFields;

    VariableReplacer(
        RexBuilder rexBuilder,
        RelDataType leftType,
        RelDataType rightType) {
      this.rexBuilder = rexBuilder;
      this.leftFields = leftType.getFieldList();
      this.rightFields = rightType.getFieldList();
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      if (index < leftFields.size()) {
        // Field came from left side of join. Move it to the right.
        return rexBuilder.makeInputRef(
            leftFields.get(index).getType(),
            rightFields.size() + index);
      }
      index -= leftFields.size();
      if (index < rightFields.size()) {
        // Field came from right side of join. Move it to the left.
        return rexBuilder.makeInputRef(
            rightFields.get(index).getType(),
            index);
      }
      throw new AssertionError("Bad field offset: index=" + inputRef.getIndex()
          + ", leftFieldCount=" + leftFields.size()
          + ", rightFieldCount=" + rightFields.size());
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinCommuteRule.Config.of()
        .withOperandFor(LogicalJoin.class)
        .withSwapOuter(false);

    Config SWAP_OUTER = DEFAULT.withSwapOuter(true);

    Config RIGHT_TO_LEFT_ONLY = DEFAULT.withRightToLeftOnly(true).withSwapOuter(true);

    @Override default JoinCommuteRule toRule() {
      return new JoinCommuteRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b ->
          b.operand(joinClass)
              // FIXME Enable this rule for joins with system fields
              .predicate(j ->
                  j.getLeft().getId() != j.getRight().getId()
                      && j.getSystemFieldList().isEmpty())
              .anyInputs())
          .as(Config.class);
    }

    /** Whether to swap outer joins; default false. */
    @Value.Default default boolean isSwapOuter() {
      return false;
    }

    /** Sets {@link #isSwapOuter()}. */
    Config withSwapOuter(boolean swapOuter);

    /** Whether to emit the new join tree if the join condition is {@code TRUE}
     * (that is, cartesian joins); default true. */
    @Value.Default default boolean isAllowAlwaysTrueCondition() {
      return true;
    }

    /** Sets {@link #isAllowAlwaysTrueCondition()}. */
    Config withAllowAlwaysTrueCondition(boolean allowAlwaysTrueCondition);

    /** If true, only RIGHT JOIN will be swapped; default false. */
    @Value.Default default boolean isRightToLeftOnly() {
      return false;
    }

    /** Sets {@link #isRightToLeftOnly()}. */
    Config withRightToLeftOnly(boolean isRightToLeftOnly);
  }
}
