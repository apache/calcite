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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that removes
 * a {@link org.apache.calcite.rel.core.Sort} if its input is already sorted.
 *
 * <p>Requires {@link RelCollationTraitDef}.
 */
public class SortRemoveRule extends RelOptRule {
  public static final SortRemoveRule INSTANCE =
      new SortRemoveRule(RelFactories.LOGICAL_BUILDER);

  /**
   * Creates a SortRemoveRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public SortRemoveRule(RelBuilderFactory relBuilderFactory) {
    super(operand(Sort.class, any()), relBuilderFactory, "SortRemoveRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    boolean sortRemoved = removeSortIfInputAlreadySorted(call);
    if (!sortRemoved) {
      // try row count based removal
      final Sort sort = call.rel(0);
      int fetch = (sort.getFetch() != null) && (sort.getFetch() instanceof RexLiteral)
          ? RexLiteral.intValue(sort.getFetch()) : -1;
      int offset = (sort.getOffset() != null) && (sort.getOffset() instanceof RexLiteral)
          ? RexLiteral.intValue(sort.getOffset()) : -1;
      Double maxRowCount = call.getMetadataQuery().getMaxRowCount(sort.getInput());
      if (shouldRemoveSortBasedOnRowCount(maxRowCount, offset, fetch)) {
        call.transformTo(sort.getInput());
      }
    }
  }

  protected  boolean removeSortIfInputAlreadySorted(RelOptRuleCall call) {
    if (!call.getPlanner().getRelTraitDefs()
        .contains(RelCollationTraitDef.INSTANCE)) {
      // Collation is not an active trait.
      return false;
    }
    final Sort sort = call.rel(0);
    if (sort.offset != null || sort.fetch != null) {
      // Don't remove sort if would also remove OFFSET or LIMIT.
      return false;
    }
    // Express the "sortedness" requirement in terms of a collation trait and
    // we can get rid of the sort. This allows us to use rels that just happen
    // to be sorted but get the same effect.
    final RelCollation collation = sort.getCollation();
    assert collation == sort.getTraitSet()
        .getTrait(RelCollationTraitDef.INSTANCE);
    final RelTraitSet traits = sort.getInput().getTraitSet().replace(collation);
    call.transformTo(convert(sort.getInput(), traits));
    return true;
  }

  // Sort is removed if all of following conditions are met
  // 1. If input's max row count is less than or equal to 1
  // 2. If fetch is greater than 0
  // 3. If offset is less than 1
  public static boolean shouldRemoveSortBasedOnRowCount(Double inputMaxRowCount,
      int offset, int fetch) {
    if (fetch < 0 && offset > 0) {
      // if limit is not defined but offset is we should bail out
      return false;
    }
    if (inputMaxRowCount != null && inputMaxRowCount <= 1) {
      if (fetch == 0) {
        return false;
      } else if (fetch > 0) {
        if (offset < 1) {
          return true;
        }
      } else {
        return true;
      }
    }
    return false;
  }
}
