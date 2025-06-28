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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Sort} past a
 * {@link org.apache.calcite.rel.core.Join}.
 *
 * <p>At the moment, we only consider left/right outer joins.
 * However, an extension for full outer joins for this rule could be envisioned.
 * Special attention should be paid to null values for correctness issues.
 *
 * @see CoreRules#SORT_JOIN_TRANSPOSE
 */
@Value.Enclosing
public class SortJoinTransposeRule
    extends RelRule<SortJoinTransposeRule.Config>
    implements TransformationRule {

  /** Creates a SortJoinTransposeRule. */
  protected SortJoinTransposeRule(Config config) {
    super(config);
  }

  /** Creates a SortJoinTransposeRule. */
  @Deprecated // to be removed before 2.0
  public SortJoinTransposeRule(Class<? extends Sort> sortClass,
      Class<? extends Join> joinClass) {
    this(Config.DEFAULT.withOperandFor(sortClass, joinClass)
        .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  public SortJoinTransposeRule(Class<? extends Sort> sortClass,
      Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withOperandFor(sortClass, joinClass)
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Join join = call.rel(1);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final JoinInfo joinInfo =
        JoinInfo.createWithStrictEquality(join.getLeft(), join.getRight(), join.getCondition());

    // 1) If sort has dynamic parameter, we bail out
    // 2) If join is not a left or right outer, we bail out
    // 3) If sort is not a trivial order-by, and if there is
    // any sort column that is not part of the input where the
    // sort is pushed, we bail out
    // 4) If sort has an offset, and if the non-preserved side
    // of the join is not count-preserving against the join
    // condition, we bail out
    if (sort.offset instanceof RexDynamicParam
        || sort.fetch instanceof RexDynamicParam) {
      return false;
    }

    if (join.getJoinType() == JoinRelType.LEFT) {
      if (sort.getCollation() != RelCollations.EMPTY) {
        for (RelFieldCollation relFieldCollation
            : sort.getCollation().getFieldCollations()) {
          if (relFieldCollation.getFieldIndex()
              >= join.getLeft().getRowType().getFieldCount()) {
            return false;
          }
        }
      }
      if (sort.offset != null
          && !RelMdUtil.areColumnsDefinitelyUnique(
              mq, join.getRight(), joinInfo.rightSet())) {
        return false;
      }
    } else if (join.getJoinType() == JoinRelType.RIGHT) {
      if (sort.getCollation() != RelCollations.EMPTY) {
        for (RelFieldCollation relFieldCollation
            : sort.getCollation().getFieldCollations()) {
          if (relFieldCollation.getFieldIndex()
              < join.getLeft().getRowType().getFieldCount()) {
            return false;
          }
        }
      }
      if (sort.offset != null
          && !RelMdUtil.areColumnsDefinitelyUnique(
              mq, join.getLeft(), joinInfo.leftSet())) {
        return false;
      }
    } else {
      return false;
    }

    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Join join = call.rel(1);

    // We create a new sort operator on the corresponding input
    final RelNode newLeftInput;
    final RelNode newRightInput;
    final RelMetadataQuery mq = call.getMetadataQuery();
    if (join.getJoinType() == JoinRelType.LEFT) {
      // If the input is already sorted and we are not reducing the number of tuples,
      // we bail out
      if (RelMdUtil.checkInputForCollationAndLimit(mq, join.getLeft(),
          sort.getCollation(), sort.offset, sort.fetch)) {
        return;
      }
      newLeftInput =
          sort.copy(sort.getTraitSet(), join.getLeft(), sort.getCollation(),
              sort.offset, sort.fetch);
      newRightInput = join.getRight();
    } else {
      final RelCollation rightCollation =
          RelCollations.shift(sort.getCollation(), -join.getLeft().getRowType().getFieldCount());
      // If the input is already sorted and we are not reducing the number of tuples,
      // we bail out
      if (RelMdUtil.checkInputForCollationAndLimit(mq, join.getRight(),
          rightCollation, sort.offset, sort.fetch)) {
        return;
      }
      newLeftInput = join.getLeft();
      newRightInput =
          sort.copy(sort.getTraitSet().replace(rightCollation),
              join.getRight(), rightCollation, sort.offset, sort.fetch);
    }
    // We copy the join and the top sort operator
    final RelNode joinCopy =
        join.copy(join.getTraitSet(), join.getCondition(), newLeftInput,
            newRightInput, join.getJoinType(), join.isSemiJoinDone());
    final RelNode sortCopy =
        sort.copy(sort.getTraitSet(), joinCopy, sort.getCollation(),
            sort.offset, sort.fetch);

    call.transformTo(sortCopy);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortJoinTransposeRule.Config.of()
        .withOperandFor(LogicalSort.class, LogicalJoin.class);

    @Override default SortJoinTransposeRule toRule() {
      return new SortJoinTransposeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Sort> sortClass,
        Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
          b0.operand(sortClass).oneInput(b1 ->
              b1.operand(joinClass).anyInputs()))
          .as(Config.class);
    }
  }
}
