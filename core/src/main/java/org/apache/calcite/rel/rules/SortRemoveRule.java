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

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that removes
 * a {@link org.apache.calcite.rel.core.Sort} if its input is already sorted.
 *
 * <p>Requires {@link RelCollationTraitDef}.
 *
 * @see CoreRules#SORT_REMOVE
 */
public class SortRemoveRule
    extends RelRule<SortRemoveRule.Config>
    implements TransformationRule {

  /** Creates a SortRemoveRule. */
  protected SortRemoveRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public SortRemoveRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
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
    final RelTraitSet traits = sort.getInput().getTraitSet()
        .replace(collation).replaceIf(ConventionTraitDef.INSTANCE, sort::getConvention);
    call.transformTo(convert(sort.getInput(), traits));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withOperandSupplier(b ->
            b.operand(Sort.class).anyInputs())
        .as(Config.class);

    @Override default SortRemoveRule toRule() {
      return new SortRemoveRule(this);
    }
  }
}
