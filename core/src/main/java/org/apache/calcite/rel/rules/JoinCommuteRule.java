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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;

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
 */
public class JoinCommuteRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** Instance of the rule that only swaps inner joins. */
  public static final JoinCommuteRule INSTANCE = new JoinCommuteRule(false);

  /** Instance of the rule that swaps outer joins as well as inner joins. */
  public static final JoinCommuteRule SWAP_OUTER = new JoinCommuteRule(true);

  private final boolean swapOuter;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a JoinCommuteRule.
   */
  public JoinCommuteRule(Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory, boolean swapOuter) {
    super(operand(clazz, any()), relBuilderFactory, null);
    this.swapOuter = swapOuter;
  }

  private JoinCommuteRule(boolean swapOuter) {
    this(LogicalJoin.class, RelFactories.LOGICAL_BUILDER, swapOuter);
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
  public static RelNode swap(Join join) {
    return swap(join, false, RelFactories.LOGICAL_BUILDER);
  }

  @Deprecated // to be removed before 2.0
  public static RelNode swap(Join join, boolean swapOuterJoins) {
    return swap(join, swapOuterJoins, RelFactories.LOGICAL_BUILDER);
  }

  /**
   * Returns a relational expression with the inputs switched round. Does not
   * modify <code>join</code>. Returns null if the join cannot be swapped (for
   * example, because it is an outer join).
   *
   * @param join              join to be swapped
   * @param swapOuterJoins    whether outer joins should be swapped
   * @param relBuilderFactory Builder for relational expressions
   * @return swapped join if swapping possible; else null
   */
  public static RelNode swap(Join join, boolean swapOuterJoins,
      RelBuilderFactory relBuilderFactory) {
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
    RexNode condition = variableReplacer.go(oldCondition);

    // NOTE jvs 14-Mar-2006: We preserve attribute semiJoinDone after the
    // swap.  This way, we will generate one semijoin for the original
    // join, and one for the swapped join, and no more.  This
    // doesn't prevent us from seeing any new combinations assuming
    // that the planner tries the desired order (semijoins after swaps).
    Join newJoin =
        join.copy(join.getTraitSet(), condition, join.getRight(),
            join.getLeft(), joinType.swap(), join.isSemiJoinDone());
    final List<RexNode> exps =
        RelOptUtil.createSwappedJoinExprs(newJoin, join, true);
    return RelOptUtil.createProject(
        newJoin, exps, join.getRowType().getFieldNames(), true,
        relBuilderFactory.create(newJoin.getCluster(), null));
  }

  public void onMatch(final RelOptRuleCall call) {
    Join join = call.rel(0);

    if (!join.getSystemFieldList().isEmpty()) {
      // FIXME Enable this rule for joins with system fields
      return;
    }

    final RelNode swapped = swap(join, this.swapOuter, relBuilderFactory);
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
   * leftFieldCount from it.</p>
   */
  private static class VariableReplacer {
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

    public RexNode go(RexNode rex) {
      if (rex instanceof RexCall) {
        ImmutableList.Builder<RexNode> builder =
            ImmutableList.builder();
        final RexCall call = (RexCall) rex;
        for (RexNode operand : call.operands) {
          builder.add(go(operand));
        }
        return call.clone(call.getType(), builder.build());
      } else if (rex instanceof RexInputRef) {
        RexInputRef var = (RexInputRef) rex;
        int index = var.getIndex();
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
        throw new AssertionError("Bad field offset: index=" + var.getIndex()
            + ", leftFieldCount=" + leftFields.size()
            + ", rightFieldCount=" + rightFields.size());
      } else {
        return rex;
      }
    }
  }
}

// End JoinCommuteRule.java
