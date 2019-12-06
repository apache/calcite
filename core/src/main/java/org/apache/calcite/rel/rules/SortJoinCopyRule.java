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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that copies a {@link org.apache.calcite.rel.core.Sort} past a
 * {@link org.apache.calcite.rel.core.Join} without its limit and offset. The
 * original {@link org.apache.calcite.rel.core.Sort} is preserved but can be
 * potentially removed by {@link org.apache.calcite.rel.rules.SortRemoveRule} if
 * redundant.
 *
 * <p>Some examples where {@link org.apache.calcite.rel.rules.SortJoinCopyRule}
 * can be useful: allowing a {@link org.apache.calcite.rel.core.Sort} to be
 * incorporated in an index scan; facilitating the use of operators requiring
 * sorted inputs; and allowing the sort to be performed on a possibly smaller
 * result.
 */
public class SortJoinCopyRule extends RelOptRule {

  public static final SortJoinCopyRule INSTANCE =
      new SortJoinCopyRule(LogicalSort.class,
          Join.class, RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /** Creates a SortJoinCopyRule. */
  protected SortJoinCopyRule(Class<? extends Sort> sortClass,
      Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
    super(
        operand(sortClass,
            operand(joinClass, any())),
        relBuilderFactory, null);
  }

  //~ Methods -----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final Join join = call.rel(1);
    final RelMetadataQuery metadataQuery = call.getMetadataQuery();

    final RelNode newLeftInput;
    final RelNode newRightInput;

    final List<RelFieldCollation> leftFieldCollation = new ArrayList<>();
    final List<RelFieldCollation> rightFieldCollation = new ArrayList<>();

    // Decompose sort collations into left and right collations
    for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
      if (relFieldCollation.getFieldIndex() >= join.getLeft().getRowType().getFieldCount()) {
        rightFieldCollation.add(relFieldCollation);
      } else {
        leftFieldCollation.add(relFieldCollation);
      }
    }

    // Add sort to new left node only if sort collations
    // contained fields from left table
    if (leftFieldCollation.isEmpty()) {
      newLeftInput = join.getLeft();
    } else {
      final RelCollation leftCollation = RelCollationTraitDef.INSTANCE.canonize(
          RelCollations.of(leftFieldCollation));
      // If left table already sorted don't add a sort
      if (RelMdUtil.checkInputForCollationAndLimit(
          metadataQuery,
          join.getLeft(),
          leftCollation,
          null,
          null)) {
        newLeftInput = join.getLeft();
      } else {
        newLeftInput = sort.copy(
            sort.getTraitSet().replaceIf(
                RelCollationTraitDef.INSTANCE,
                () -> leftCollation),
            join.getLeft(),
            leftCollation,
            null,
            null);
      }
    }
    // Add sort to new right node only if sort collations
    // contained fields from right table
    if (rightFieldCollation.isEmpty()) {
      newRightInput = join.getRight();
    } else {
      final RelCollation rightCollation = RelCollationTraitDef.INSTANCE.canonize(
          RelCollations.shift(
              RelCollations.of(rightFieldCollation),
              -join.getLeft().getRowType().getFieldCount()));
      // If right table already sorted don't add a sort
      if (RelMdUtil.checkInputForCollationAndLimit(
          metadataQuery,
          join.getRight(),
          rightCollation,
          null,
          null)) {
        newRightInput = join.getRight();
      } else {
        newRightInput = sort.copy(
            sort.getTraitSet().replaceIf(
                RelCollationTraitDef.INSTANCE,
                () -> rightCollation),
            join.getRight(),
            rightCollation,
            null,
            null);
      }
    }
    // If no change was made no need to apply the rule
    if (newLeftInput == join.getLeft() && newRightInput == join.getRight()) {
      return;
    }

    final RelNode joinCopy = join.copy(
        join.getTraitSet(),
        join.getCondition(),
        newLeftInput,
        newRightInput,
        join.getJoinType(),
        join.isSemiJoinDone());
    final RelNode sortCopy = sort.copy(
        sort.getTraitSet(),
        joinCopy,
        sort.getCollation(),
        sort.offset,
        sort.fetch);

    call.transformTo(sortCopy);
  }
}

// End SortJoinCopyRule.java
