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
    if (!call.getPlanner().getRelTraitDefs()
        .contains(RelCollationTraitDef.INSTANCE)) {
      // Collation is not an active trait.
      return;
    }
    final Sort sort = call.rel(0);
    if (sort.offset != null || sort.fetch != null) {
      // Don't remove sort if would also remove OFFSET or LIMIT.
      return;
    }
    // Express the "sortedness" requirement in terms of a collation trait and
    // we can get rid of the sort. This allows us to use rels that just happen
    // to be sorted but get the same effect.
    final RelCollation collation = sort.getCollation();
    assert collation == sort.getTraitSet()
        .getTrait(RelCollationTraitDef.INSTANCE);
    final RelTraitSet traits = sort.getInput().getTraitSet().replace(collation);
    call.transformTo(convert(sort.getInput(), traits));
  }
}

// End SortRemoveRule.java
