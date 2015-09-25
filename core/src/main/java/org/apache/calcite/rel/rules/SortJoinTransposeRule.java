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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;


/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Sort} past a
 * {@link org.apache.calcite.rel.core.Join}. At the moment, we only consider
 * left/right outer joins.
 * However, an extesion for full outer joins for this rule could be envision.
 * Special attention should be paid to null values for correctness issues.
 */
public class SortJoinTransposeRule extends RelOptRule {

  public static final SortJoinTransposeRule INSTANCE =
      new SortJoinTransposeRule(LogicalSort.class,
              LogicalJoin.class);

  //~ Constructors -----------------------------------------------------------

  /** Creates a SortJoinTransposeRule. */
  public SortJoinTransposeRule(Class<? extends Sort> sortClass,
          Class<? extends Join> joinClass) {
    super(
        operand(sortClass,
            operand(joinClass, any())));
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Join join = call.rel(1);

    // 1) If join is not a left or right outer, we bail out
    // 2) If sort does not consist only of a limit operation,
    // or any sort column is not part of the input where the
    // sort is pushed, we bail out
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
    } else {
      return false;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Join join = call.rel(1);

    // We create a new sort operator on the corresponding input
    final RelNode newLeftInput;
    final RelNode newRightInput;
    if (join.getJoinType() == JoinRelType.LEFT) {
      // If the input is already sorted and we are not reducing the number of tuples,
      // we bail out
      if (checkInputForCollationAndLimit(join.getLeft(), sort.getCollation(),
          sort.offset, sort.fetch)) {
        return;
      }
      newLeftInput = sort.copy(sort.getTraitSet(), join.getLeft(), sort.getCollation(),
              sort.offset, sort.fetch);
      newRightInput = join.getRight();
    } else {
      final RelCollation rightCollation = RelCollations.shift(
          sort.getCollation(), -join.getLeft().getRowType().getFieldCount());
      // If the input is already sorted and we are not reducing the number of tuples,
      // we bail out
      if (checkInputForCollationAndLimit(join.getRight(), rightCollation,
          sort.offset, sort.fetch)) {
        return;
      }
      newLeftInput = join.getLeft();
      newRightInput = sort.copy(sort.getTraitSet(), join.getRight(), rightCollation,
              sort.offset, sort.fetch);
    }
    // We copy the join and the top sort operator
    final RelNode joinCopy = join.copy(join.getTraitSet(), join.getCondition(), newLeftInput,
            newRightInput, join.getJoinType(), join.isSemiJoinDone());
    final RelNode sortCopy = sort.copy(sort.getTraitSet(), joinCopy, sort.getCollation(),
            sort.offset, sort.fetch);

    call.transformTo(sortCopy);
  }

  /* Checks if an input is already sorted and has less rows than
   * the sum of offset and limit */
  private boolean checkInputForCollationAndLimit(RelNode input,
      RelCollation collation, RexNode offset, RexNode fetch) {
    // Check if the input is already sorted
    ImmutableList<RelCollation> inputCollation =
        RelMetadataQuery.collations(input);
    final boolean alreadySorted = RelCollations.equal(
        ImmutableList.of(collation), inputCollation);
    // Check if we are not reducing the number of tuples
    boolean alreadySmaller = true;
    final Double rowCount = RelMetadataQuery.getRowCount(input);
    if (rowCount != null && fetch != null) {
      final int offsetVal = offset == null ? 0 : RexLiteral.intValue(offset);
      final int limit = RexLiteral.intValue(fetch);
      final Double offsetLimit = new Double(offsetVal + limit);
      if (offsetLimit < rowCount) {
        alreadySmaller = false;
      }
    }
    return alreadySorted && alreadySmaller;
  }

}
