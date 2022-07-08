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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

/**
 * Planner rule that pushes filters above and
 * within a join node into the join node and/or its children nodes.
 *
 * @param <C> Configuration type
 */
public abstract class FilterJoinRule<C extends FilterJoinRule.Config>
    extends RelRule<C>
    implements TransformationRule {
  /** Predicate that always returns true. With this predicate, every filter
   * will be pushed into the ON clause. */
  @Deprecated // to be removed before 2.0
  public static final Predicate TRUE_PREDICATE = (join, joinType, exp) -> true;

  /** Creates a FilterJoinRule. */
  protected FilterJoinRule(C config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  protected void perform(RelOptRuleCall call, @Nullable Filter filter,
      Join join) {
    List<RexNode> joinFilters =
        RelOptUtil.conjunctions(join.getCondition());
    final List<RexNode> origJoinFilters = ImmutableList.copyOf(joinFilters);

    // If there is only the joinRel,
    // make sure it does not match a cartesian product joinRel
    // (with "true" condition), otherwise this rule will be applied
    // again on the new cartesian product joinRel.
    if (filter == null && joinFilters.isEmpty()) {
      return;
    }

    final List<RexNode> aboveFilters =
        filter != null
            ? getConjunctions(filter)
            : new ArrayList<>();
    final ImmutableList<RexNode> origAboveFilters =
        ImmutableList.copyOf(aboveFilters);

    // Simplify Outer Joins
    JoinRelType joinType = join.getJoinType();
    if (config.isSmart()
        && !origAboveFilters.isEmpty()
        && join.getJoinType() != JoinRelType.INNER) {
      joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
    }

    final List<RexNode> leftFilters = new ArrayList<>();
    final List<RexNode> rightFilters = new ArrayList<>();

    // TODO - add logic to derive additional filters.  E.g., from
    // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
    // derive table filters:
    // (t1.a = 1 OR t1.b = 3)
    // (t2.a = 2 OR t2.b = 4)

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    boolean filterPushed = false;
    if (RelOptUtil.classifyFilters(
        join,
        aboveFilters,
        joinType.canPushIntoFromAbove(),
        joinType.canPushLeftFromAbove(),
        joinType.canPushRightFromAbove(),
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // Move join filters up if needed
    validateJoinFilters(aboveFilters, joinFilters, join, joinType);

    // If no filter got pushed after validate, reset filterPushed flag
    if (leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinFilters.size() == origJoinFilters.size()
        && aboveFilters.size() == origAboveFilters.size()) {
      if (Sets.newHashSet(joinFilters)
          .equals(Sets.newHashSet(origJoinFilters))) {
        filterPushed = false;
      }
    }

    if (joinType != JoinRelType.FULL) {
      joinFilters = inferJoinEqualConditions(joinFilters, join);
    }

    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.

    // Anti-join on conditions can not be pushed into left or right, e.g. for plan:
    //
    //     Join(condition=[AND(cond1, $2)], joinType=[anti])
    //     :  - prj(f0=[$0], f1=[$1], f2=[$2])
    //     :  - prj(f0=[$0])
    //
    // The semantic would change if join condition $2 is pushed into left,
    // that is, the result set may be smaller. The right can not be pushed
    // into for the same reason.
    if (RelOptUtil.classifyFilters(
        join,
        joinFilters,
        false,
        joinType.canPushLeftFromWithin(),
        joinType.canPushRightFromWithin(),
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // if nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if ((!filterPushed
            && joinType == join.getJoinType())
        || (joinFilters.isEmpty()
            && leftFilters.isEmpty()
            && rightFilters.isEmpty())) {
      return;
    }

    // create Filters on top of the children if any filters were
    // pushed to them
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    final RelNode leftRel =
        relBuilder.push(join.getLeft()).filter(leftFilters).build();
    final RelNode rightRel =
        relBuilder.push(join.getRight()).filter(rightFilters).build();

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    final ImmutableList<RelDataType> fieldTypes =
        ImmutableList.<RelDataType>builder()
            .addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
            .addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType())).build();
    final RexNode joinFilter =
        RexUtil.composeConjunction(rexBuilder,
            RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes));

    // If nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if (joinFilter.isAlwaysTrue()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinType == join.getJoinType()) {
      return;
    }

    RelNode newJoinRel =
        join.copy(
            join.getTraitSet(),
            joinFilter,
            leftRel,
            rightRel,
            joinType,
            join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newJoinRel);
    if (!leftFilters.isEmpty() && filter != null) {
      call.getPlanner().onCopy(filter, leftRel);
    }
    if (!rightFilters.isEmpty() && filter != null) {
      call.getPlanner().onCopy(filter, rightRel);
    }

    relBuilder.push(newJoinRel);

    // Create a project on top of the join if some of the columns have become
    // NOT NULL due to the join-type getting stricter.
    relBuilder.convert(join.getRowType(), false);

    // create a FilterRel on top of the join if needed
    relBuilder.filter(
        RexUtil.fixUp(rexBuilder, aboveFilters,
            RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));
    call.transformTo(relBuilder.build());
  }

  /**
   * Infers more equal conditions for the join condition.
   *
   * <p> For example, in {@code SELECT * FROM T1, T2, T3 WHERE T1.id = T3.id AND T2.id = T3.id},
   * we can infer {@code T1.id = T2.id} for the first Join node from second Join node's condition:
   * {@code T1.id = T3.id AND T2.id = T3.id}.
   *
   * <p>For the above SQL, the second Join's condition is {@code T1.id = T3.id AND T2.id = T3.id}.
   * After inference, the final condition would be: {@code T1.id = T2.id AND T1.id = T3.id}, the
   * {@code T1.id = T2.id} can be further pushed into LHS.
   *
   * @param rexNodes the Join condition
   * @param join the Join node
   * @return the newly inferred conditions
   */
  protected List<RexNode> inferJoinEqualConditions(List<RexNode> rexNodes, Join join) {
    final List<RexNode> result = new ArrayList<>(rexNodes.size());
    final List<Set<RexInputRef>> equalSets = splitEqualSets(rexNodes, result);

    boolean needOptimize = false;
    for (Set<RexInputRef> set : equalSets) {
      if (set.size() > 2) {
        needOptimize = true;
        break;
      }
    }
    if (!needOptimize) {
      // Keep the conditions unchanged.
      return rexNodes;
    }

    result.addAll(constructConditionFromEqualSets(join, equalSets));
    return result;
  }

  /**
   * Splits out the equal sets.
   *
   * @param rexNodes the original conditions
   * @param leftNodes where the conditions not feasible for equal sets are put
   * @return the equal sets
   */
  private static List<Set<RexInputRef>> splitEqualSets(List<RexNode> rexNodes,
      List<RexNode> leftNodes) {
    final List<Set<RexInputRef>> equalSets = new ArrayList<>();
    for (RexNode rexNode : rexNodes) {
      if (rexNode.isA(SqlKind.EQUALS)) {
        final RexNode op1 = ((RexCall) rexNode).getOperands().get(0);
        final RexNode op2 = ((RexCall) rexNode).getOperands().get(1);
        if (op1 instanceof RexInputRef && op2 instanceof RexInputRef) {
          final RexInputRef in1 = (RexInputRef) op1;
          final RexInputRef in2 = (RexInputRef) op2;
          Set<RexInputRef> set = null;
          for (Set<RexInputRef> s : equalSets) {
            if (s.contains(in1) || s.contains(in2)) {
              set = s;
              break;
            }
          }
          if (set == null) {
            // To make the result deterministic.
            set = new LinkedHashSet<>();
            equalSets.add(set);
          }
          set.add(in1);
          set.add(in2);
        } else {
          leftNodes.add(rexNode);
        }
      } else {
        leftNodes.add(rexNode);
      }
    }

    return equalSets;
  }

  /**
   * Constructs new equal conditions from the equal sets.
   *
   * @param join the original {@link Join} node
   * @param equalSets the equal sets
   * @return the newly constructed conditions from equal sets
   */
  private List<RexNode> constructConditionFromEqualSets(Join join,
      List<Set<RexInputRef>> equalSets) {
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final List<RexNode> result = new ArrayList<>();
    final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    for (Set<RexInputRef> set : equalSets) {
      final List<RexInputRef> leftSet = new ArrayList<>();
      final List<RexInputRef> rightSet = new ArrayList<>();
      for (RexInputRef ref : set) {
        if (ref.getIndex() < leftFieldCount) {
          leftSet.add(ref);
        } else {
          rightSet.add(ref);
        }
      }
      // Add left side conditions.
      if (leftSet.size() > 1) {
        for (int i = 1; i < leftSet.size(); ++i) {
          result.add(
              rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                  leftSet.get(0),
                  leftSet.get(i)));
        }
      }
      // Add right side conditions.
      if (rightSet.size() > 1) {
        for (int i = 1; i < rightSet.size(); ++i) {
          result.add(
              rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                  rightSet.get(0),
                  rightSet.get(i)));
        }
      }
      // Only need one equal condition for each equal set.
      if (leftSet.size() > 0 && rightSet.size() > 0) {
        result.add(
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                leftSet.get(0),
                rightSet.get(0)));
      }
    }

    return result;
  }

  /**
   * Get conjunctions of filter's condition but with collapsed
   * {@code IS NOT DISTINCT FROM} expressions if needed.
   *
   * @param filter filter containing condition
   * @return condition conjunctions with collapsed {@code IS NOT DISTINCT FROM}
   * expressions if any
   * @see RelOptUtil#conjunctions(RexNode)
   */
  private static List<RexNode> getConjunctions(Filter filter) {
    List<RexNode> conjunctions = conjunctions(filter.getCondition());
    RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    for (int i = 0; i < conjunctions.size(); i++) {
      RexNode node = conjunctions.get(i);
      if (node instanceof RexCall) {
        conjunctions.set(i,
            RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node, rexBuilder));
      }
    }
    return conjunctions;
  }

  /**
   * Validates that target execution framework can satisfy join filters.
   *
   * <p>If the join filter cannot be satisfied (for example, if it is
   * {@code l.c1 > r.c2} and the join only supports equi-join), removes the
   * filter from {@code joinFilters} and adds it to {@code aboveFilters}.
   *
   * <p>The default implementation does nothing; i.e. the join can handle all
   * conditions.
   *
   * @param aboveFilters Filter above Join
   * @param joinFilters Filters in join condition
   * @param join Join
   * @param joinType JoinRelType could be different from type in Join due to
   * outer join simplification.
   */
  protected void validateJoinFilters(List<RexNode> aboveFilters,
      List<RexNode> joinFilters, Join join, JoinRelType joinType) {
    final Iterator<RexNode> filterIter = joinFilters.iterator();
    while (filterIter.hasNext()) {
      RexNode exp = filterIter.next();
      // Do not pull up filter conditions for semi/anti join.
      if (!config.getPredicate().apply(join, joinType, exp)
          && joinType.projectsRight()) {
        aboveFilters.add(exp);
        filterIter.remove();
      }
    }
  }

  /** Rule that pushes parts of the join condition to its inputs. */
  public static class JoinConditionPushRule
      extends FilterJoinRule<JoinConditionPushRule.JoinConditionPushRuleConfig> {
    /** Creates a JoinConditionPushRule. */
    protected JoinConditionPushRule(JoinConditionPushRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public JoinConditionPushRule(RelBuilderFactory relBuilderFactory,
        Predicate predicate) {
      this(ImmutableJoinConditionPushRuleConfig.of(predicate)
          .withRelBuilderFactory(relBuilderFactory)
          .withOperandSupplier(b ->
              b.operand(Join.class).anyInputs())
          .withDescription("FilterJoinRule:no-filter")
          .withSmart(true));
    }

    @Deprecated // to be removed before 2.0
    public JoinConditionPushRule(RelFactories.FilterFactory filterFactory,
        RelFactories.ProjectFactory projectFactory, Predicate predicate) {
      this(RelBuilder.proto(filterFactory, projectFactory), predicate);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Join join = call.rel(0);
      perform(call, null, join);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface JoinConditionPushRuleConfig extends FilterJoinRule.Config {
      JoinConditionPushRuleConfig DEFAULT = ImmutableJoinConditionPushRuleConfig
          .of((join, joinType, exp) -> true)
          .withOperandSupplier(b ->
              b.operand(Join.class).anyInputs())
          .withSmart(true);

      @Override default JoinConditionPushRule toRule() {
        return new JoinConditionPushRule(this);
      }
    }
  }

  /** Rule that tries to push filter expressions into a join
   * condition and into the inputs of the join.
   *
   * @see CoreRules#FILTER_INTO_JOIN */
  public static class FilterIntoJoinRule
      extends FilterJoinRule<FilterIntoJoinRule.FilterIntoJoinRuleConfig> {
    /** Creates a FilterIntoJoinRule. */
    protected FilterIntoJoinRule(FilterIntoJoinRuleConfig config) {
      super(config);
    }

    @Deprecated // to be removed before 2.0
    public FilterIntoJoinRule(boolean smart,
        RelBuilderFactory relBuilderFactory, Predicate predicate) {
      this(ImmutableFilterIntoJoinRuleConfig.of(predicate)
          .withRelBuilderFactory(relBuilderFactory)
          .withOperandSupplier(b0 ->
              b0.operand(Filter.class).oneInput(b1 ->
                  b1.operand(Join.class).anyInputs()))
          .withDescription("FilterJoinRule:filter")
          .withSmart(smart));
    }

    @Deprecated // to be removed before 2.0
    public FilterIntoJoinRule(boolean smart,
        RelFactories.FilterFactory filterFactory,
        RelFactories.ProjectFactory projectFactory,
        Predicate predicate) {
      this(ImmutableFilterIntoJoinRuleConfig.of(predicate)
          .withRelBuilderFactory(
              RelBuilder.proto(filterFactory, projectFactory))
          .withOperandSupplier(b0 ->
              b0.operand(Filter.class).oneInput(b1 ->
                  b1.operand(Join.class).anyInputs()))
          .withDescription("FilterJoinRule:filter")
          .withSmart(smart));
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      Join join = call.rel(1);
      perform(call, filter, join);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface FilterIntoJoinRuleConfig extends FilterJoinRule.Config {
      FilterIntoJoinRuleConfig DEFAULT =
          ImmutableFilterIntoJoinRuleConfig.of((join, joinType, exp) -> true)
              .withOperandSupplier(b0 ->
                  b0.operand(Filter.class).oneInput(b1 ->
                      b1.operand(Join.class).anyInputs()))
              .withSmart(true);

      @Override default FilterIntoJoinRule toRule() {
        return new FilterIntoJoinRule(this);
      }
    }
  }

  /** Predicate that returns whether a filter is valid in the ON clause of a
   * join for this particular kind of join. If not, Calcite will push it back to
   * above the join. */
  @FunctionalInterface
  public interface Predicate {
    boolean apply(Join join, JoinRelType joinType, RexNode exp);
  }

  /**
   * Rule configuration.
   */
  public interface Config extends RelRule.Config {
    /** Whether to try to strengthen join-type, default false. */
    @Value.Default default boolean isSmart() {
      return false;
    }

    /** Sets {@link #isSmart()}. */
    Config withSmart(boolean smart);

    /** Predicate that returns whether a filter is valid in the ON clause of a
     * join for this particular kind of join. If not, Calcite will push it back to
     * above the join. */
    @Value.Parameter
    Predicate getPredicate();

    /** Sets {@link #getPredicate()} ()}. */
    Config withPredicate(Predicate predicate);
  }
}
