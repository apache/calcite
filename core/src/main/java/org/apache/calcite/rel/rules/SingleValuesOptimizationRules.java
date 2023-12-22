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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Collection of rules which simplify joins which have one of their input as constant relations
 * {@link Values} that produce a single row.
 *
 * <p>Conventionally, the way to represent a single row constant relational expression is
 * with a {@link Values} that has one tuple.
 *
 * @see LogicalValues#createOneRow
 */
public abstract class SingleValuesOptimizationRules {

  public static final RelOptRule JOIN_LEFT_INSTANCE =
      SingleValuesOptimizationRules.JoinLeftSingleRuleConfig.DEFAULT.toRule();

  public static final RelOptRule JOIN_RIGHT_INSTANCE =
      SingleValuesOptimizationRules.JoinRightSingleRuleConfig.DEFAULT.toRule();

  public static final RelOptRule JOIN_LEFT_PROJECT_INSTANCE =
      SingleValuesOptimizationRules.JoinLeftSingleValueRuleWithExprConfig.DEFAULT.toRule();

  public static final RelOptRule JOIN_RIGHT_PROJECT_INSTANCE =
      SingleValuesOptimizationRules.JoinRightSingleValueRuleWithExprConfig.DEFAULT.toRule();

  /**
   * Transformer class to transform a Join rel node tree with single constant row {@link Values}
   * on either side of the Join to a simplified tree without a Join rel node.
   */
  private static class SingleValuesRelTransformer {

    private final Join join;
    private final RelNode relNode;
    private final Predicate<Join> transformable;
    private final BiFunction<RexNode, List<RexNode>, List<RexNode>> litTransformer;
    private final boolean valuesAsLeftChild;
    private final List<RexNode> literals;

    /**
     * A transformer object which transforms a Join rel node tree with constant relation
     * node as one of its input to a rel node tree without a Join.
     *
     * @param join Join which is eligible for removal.
     * @param rexNodes List of the expressions that are part of Project
     * @param otherNode RelNode which is other side of the Join (apart from Values node)
     * @param transformable Predicate to check if the given Join is transformable or not.
     * @param isValuesLeftChild TRUE if Values is left child of join, FALSE otherwise.
     * @param litTransformer A transformer function supplied by the caller.
     *                       This function is specific to Join Type.
     *                       LEFT/ RIGHT => has logic to produce null values for unmatched rows.
     *                       INNER => produce the rexLiterals specified in the Values node.
     */
    protected SingleValuesRelTransformer(
        Join join, List<RexNode> rexNodes, RelNode otherNode,
        Predicate<Join> transformable, boolean isValuesLeftChild,
        BiFunction<RexNode, List<RexNode>, List<RexNode>> litTransformer) {
      this.relNode = otherNode;
      this.join = join;
      this.transformable = transformable;
      this.litTransformer = litTransformer;
      this.valuesAsLeftChild = isValuesLeftChild;
      this.literals = rexNodes;
    }

    /**
     * A transform function which simplifies the join tree when eligibility criteria is met.
     * Refer to {@link PruneSingleValueRule#onMatch} function for details about the transformation.

     * Another very subtle point is that this works even if the VALUES contain NULL entries.
     * This is because this generates code with comparisons to NULL, and comparing a value to NULL
     * using = always returns false, so the result is an empty collection, as expected.
     *
     * @param relBuilder Relation Builder supplied by the planner framework.
     * @return Simplified relNode tree by replacing Join,
     *          Otherwise returns null when criteria is not met.
     */
    public @Nullable RelNode transform(RelBuilder relBuilder) {
      if (!transformable.test(join)) {
        return null;
      }
      int end = valuesAsLeftChild
          ? join.getLeft().getRowType().getFieldCount()
          : join.getRowType().getFieldCount();

      int start = valuesAsLeftChild
          ? 0
          : join.getLeft().getRowType().getFieldCount();
      ImmutableBitSet bitSet = ImmutableBitSet.range(start, end);
      RexNode trueNode = relBuilder.getRexBuilder().makeLiteral(true);
      final RexNode filterCondition =
          new RexNodeReplacer(bitSet,
              literals,
              (valuesAsLeftChild ? 0 : -1) * join.getLeft().getRowType().getFieldCount())
              .go(join.getCondition());

      RexNode fixedCondition =
          valuesAsLeftChild
              ? RexUtil.shift(filterCondition,
              -1 * join.getLeft().getRowType().getFieldCount())
              : filterCondition;

      List<RexNode> rexLiterals = litTransformer.apply(fixedCondition, literals);
      relBuilder.push(relNode)
          .filter(join.getJoinType().isOuterJoin() ? trueNode : fixedCondition);

      List<RexNode> rexNodes = relNode
          .getRowType()
          .getFieldList()
          .stream()
          .map(fld -> relBuilder.field(fld.getIndex()))
          .collect(Collectors.toList());

      List<RexNode> projects = new ArrayList<>();
      projects.addAll(valuesAsLeftChild ? rexLiterals : rexNodes);
      projects.addAll(valuesAsLeftChild ? rexNodes : rexLiterals);
      return relBuilder.project(projects).build();
    }
  }

  /**
   * A rex shuttle to replace field refs with supplied rexNodes from a {@link Values} row.
   */
  private static class RexNodeReplacer extends RexShuttle {

    private final ImmutableBitSet bitSet;
    private final List<RexNode> fieldValues;
    private final int offset;

    /**
     * A RexNode replacer which replaces an inputRef with a corresponding
     * RexNode value supplied in values parameter of the constructor.
     *
     * @param bitSet A bitmap to track indices of the inputRef that get replaced.
     * @param values RexNodes list that are used to replace inputRefs.
     * @param offset offset to be applied for the inputRef index to get the corresponding
     *              RexNode value.
     */
    RexNodeReplacer(ImmutableBitSet bitSet, List<RexNode> values, int offset) {
      this.bitSet = bitSet;
      this.fieldValues = values;
      this.offset = offset;
    }
    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      if (bitSet.get(inputRef.getIndex())) {
        return this.fieldValues.get(inputRef.getIndex() + offset);
      }
      return super.visitInputRef(inputRef);
    }

    public RexNode go(RexNode expression) {
      return expression.accept(this);
    }
  }

  /**
   * Abstract class for all the SingleValuesOptimizationRules. All the specific
   * SingleValuesOptimizationRules call the onMatch function to replace a Join
   * with equivalent tree when eligible.
   */
  protected abstract static class PruneSingleValueRule
      extends RelRule<PruneSingleValueRule.Config>
      implements SubstitutionRule {
    protected PruneSingleValueRule(PruneSingleValueRule.Config config) {
      super(config);
    }

    protected BiFunction<RexNode, List<RexNode>, List<RexNode>>
        getRexTransformer(RexBuilder rexBuilder,
        JoinRelType joinRelType) {
      switch (joinRelType) {
      case LEFT:
      case RIGHT:
        return (condition, rexLiterals) -> rexLiterals.stream().map(lit ->
            rexBuilder.makeCall(SqlStdOperatorTable.CASE, condition,
                lit, rexBuilder.makeNullLiteral(lit.getType()))).collect(Collectors.toList());
      default:
        return (condition, rexLiterals) -> rexLiterals;
      }
    }

    /**
     * onMatch function contains common optimization logic for all the
     * SingleValueOptimization rules. It simplifies the rel node tree by
     * removing a Join node and creating a required filter as applicable.
     * In case of the LEFT/RIGHT joins, a case expression which produce NULL
     * values for non-matching rows will be created as part of a Project node.

     * The transformation can be illustrated by following e.g.

     *  TableScan(Emp) join (cond: Emp.empno = v.no) with Values(1010 as no) &rarr;
     *  Filter(Emp.empno = 1010) on TableScan(Emp)
     *
     * @param call          A RelOptRuleCall object
     * @param values        A constant relation node which produces a single row.
     * @param project       A project node which has dynamic constants (can be null).
     * @param join          A join node which will get removed.
     * @param other         A node on the other side of the join (apart from Values)
     * @param isOnLefSide   Whether a Values node is on the Left side or the right side
     *                      of the Join.
     */
    protected void onMatch(RelOptRuleCall call, Values values,
        @Nullable Project project, Join join,
        RelNode other, boolean isOnLefSide) {
      Predicate<Join> transformableCheck = isJoinTransformable(isOnLefSide);
      List<RexNode> rexNodes;
      if (project != null) {
        ImmutableBitSet bitSet = ImmutableBitSet.range(0, values.getRowType().getFieldCount());
        RexShuttle shuttle =
            new RexNodeReplacer(bitSet,
                    new ArrayList<>(values.tuples.get(0)),
                0);

        rexNodes = project.getProjects().stream()
            .map(shuttle::apply)
            .collect(Collectors.toList());
      } else {
        rexNodes = new ArrayList<>(values.tuples.get(0));
      }
      RelBuilder relBuilder = call.builder();

      BiFunction<RexNode, List<RexNode>, List<RexNode>> transformer =
          getRexTransformer(relBuilder.getRexBuilder(), join.getJoinType());

      SingleValuesRelTransformer relTransformer =
          new SingleValuesRelTransformer(join, rexNodes, other,
              transformableCheck, isOnLefSide, transformer);

      RelNode transformedRelNode =
          relTransformer.transform(relBuilder);

      if (transformedRelNode != null) {
        call.transformTo(transformedRelNode);
      }
    }

    static Predicate<Join> isJoinTransformable(boolean isLeft) {

      Predicate<Join> isFullOrAntiJoin =
          jn -> jn.getJoinType() == JoinRelType.ANTI
          || jn.getJoinType() == JoinRelType.FULL;

      if (isLeft) {
        return jn -> !(jn.getJoinType() == JoinRelType.LEFT || isFullOrAntiJoin.test(jn));
      } else {
        return jn -> !(jn.getJoinType() == JoinRelType.RIGHT  || isFullOrAntiJoin.test(jn));
      }
    }

    @Override public boolean autoPruneOld() {
      return true;
    }

    /** Rule configuration. */
    protected interface Config extends RelRule.Config {
      @Override PruneSingleValueRule toRule();
    }
  }

  /** Configuration for a rule that simplifies join node with constant row on its right input. */
  @Value.Immutable
  interface JoinRightSingleRuleConfig extends PruneSingleValueRule.Config {
    JoinRightSingleRuleConfig DEFAULT = ImmutableJoinRightSingleRuleConfig.of()
        .withOperandSupplier(b0 ->
            b0.operand(Join.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Values.class).predicate(Values::isSingleValue).noInputs()))
        .withDescription("PruneJoinSingleValue(right)");

    @Override default SingleValuesOptimizationRules.PruneSingleValueRule toRule() {
      return new SingleValuesOptimizationRules.PruneSingleValueRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final Values values = call.rel(2);
          final RelNode other = call.rel(1);
          onMatch(call, values, null, join, other, false);
        }
      };
    }
  }

  /** Configuration for a rule that simplifies join node with constant row on its left input. */
  @Value.Immutable
  interface JoinLeftSingleRuleConfig extends PruneSingleValueRule.Config {
    JoinLeftSingleRuleConfig DEFAULT = ImmutableJoinLeftSingleRuleConfig.of()
        .withOperandSupplier(b0 ->
            b0.operand(Join.class).inputs(
                b1 -> b1.operand(Values.class).predicate(Values::isSingleValue).noInputs(),
                b2 -> b2.operand(RelNode.class).anyInputs()))
        .withDescription("PruneJoinSingleValueRule(left)");

    @Override default SingleValuesOptimizationRules.PruneSingleValueRule toRule() {
      return new SingleValuesOptimizationRules.PruneSingleValueRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final Values values = call.rel(1);
          final RelNode other = call.rel(2);
          onMatch(call, values, null, join, other, true);
        }
      };
    }
  }

  /** Configuration for a rule that simplifies join node with a project on a constant row
   *  on its left input. A project node shows up for use cases with dynamic constants like
   *  current_timestamp(dynamic constants are not RexLiteral so cannot be part of Values node).*/
  @Value.Immutable
  interface JoinLeftSingleValueRuleWithExprConfig extends PruneSingleValueRule.Config {
    JoinLeftSingleValueRuleWithExprConfig DEFAULT =
        ImmutableJoinLeftSingleValueRuleWithExprConfig.of().withOperandSupplier(b0 ->
            b0.operand(Join.class).inputs(
                b1 -> b1.operand(Project.class).inputs(
                    b11 -> b11.operand(Values.class).predicate(Values::isSingleValue).noInputs()),
                b2 -> b2.operand(RelNode.class).anyInputs()))
        .withDescription("PruneJoinSingleValueRuleWithExpr(left)");

    @Override default SingleValuesOptimizationRules.PruneSingleValueRule toRule() {
      return new SingleValuesOptimizationRules.PruneSingleValueRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final Project project = call.rel(1);
          final Values values = call.rel(2);
          final RelNode other = call.rel(3);
          onMatch(call, values, project, join, other, true);
        }
      };
    }
  }

  /** Configuration for a rule that simplifies join node with a project on a constant row
   *  on its right input. A project node shows up for use cases with dynamic constants like
   *  current_timestamp(dynamic constants are not RexLiteral so cannot be part of Values node).*/
  @Value.Immutable
  interface JoinRightSingleValueRuleWithExprConfig extends PruneSingleValueRule.Config {
    JoinRightSingleValueRuleWithExprConfig DEFAULT =
        ImmutableJoinRightSingleValueRuleWithExprConfig.of().withOperandSupplier(b0 ->
            b0.operand(Join.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Project.class).inputs(
                    b21 -> b21.operand(Values.class).predicate(Values::isSingleValue).noInputs())))
        .withDescription("PruneJoinSingleValueRuleWithExpr(right)");

    @Override default SingleValuesOptimizationRules.PruneSingleValueRule toRule() {
      return new SingleValuesOptimizationRules.PruneSingleValueRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          final Join join = call.rel(0);
          final RelNode other = call.rel(1);
          final Project project = call.rel(2);
          final Values values = call.rel(3);
          onMatch(call, values, project, join, other, false);
        }
      };
    }
  }
}
