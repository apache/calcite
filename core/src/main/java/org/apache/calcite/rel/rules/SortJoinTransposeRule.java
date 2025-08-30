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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.math.BigDecimal;

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

    // Do nothing if sort offset/fetch is dynamic param
    if (sort.offset instanceof RexDynamicParam || sort.fetch instanceof RexDynamicParam) {
      return false;
    }

    final JoinRelType joinType = join.getJoinType();
    final boolean isLeft = joinType == JoinRelType.LEFT;
    final boolean isRight = joinType == JoinRelType.RIGHT;
    final RelCollation collation = sort.getCollation();

    if (!isLeft && !isRight) {
      return false;
    }

    // Check collation fields if not trivial order-by
    if (collation != RelCollations.EMPTY) {
      final int leftFieldCnt = join.getLeft().getRowType().getFieldCount();
      for (RelFieldCollation fc : collation.getFieldCollations()) {
        int idx = fc.getFieldIndex();
        if (isLeft && idx >= leftFieldCnt) {
          return false;
        }
        if (isRight && idx < leftFieldCnt) {
          return false;
        }
      }
    } else if (sort.fetch == null) {
      // If no order-by, must have limit
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

      final RexNode newFetch =
          calculateInnerSortFetch(sort, call.builder().getRexBuilder());
      newLeftInput =
          sort.copy(sort.getTraitSet(), join.getLeft(), sort.getCollation(), null, newFetch);
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
      final RexNode newFetch =
          calculateInnerSortFetch(sort, call.builder().getRexBuilder());
      newRightInput =
          sort.copy(sort.getTraitSet().replace(rightCollation), join.getRight(), rightCollation,
              null, newFetch);
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

  /**
   * Returns the fetch value for the inner sort when pushing sort past join.
   * The value is outer sort's offset + fetch.
   *
   * @param sort the outer sort
   * @param rexBuilder RexBuilder to create literals
   * @return fetch for inner sort
   */
  private static @Nullable RexNode calculateInnerSortFetch(Sort sort, RexBuilder rexBuilder) {
    if (sort.fetch == null) {
      return null;
    }
    final long outerFetch = RexLiteral.longValue(sort.fetch);
    final long outerOffset = sort.offset != null ? RexLiteral.longValue(sort.offset) : 0;
    final long totalFetch = outerOffset + outerFetch;
    return rexBuilder.makeExactLiteral(BigDecimal.valueOf(totalFetch));
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
