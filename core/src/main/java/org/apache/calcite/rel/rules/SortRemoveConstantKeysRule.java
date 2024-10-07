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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.util.Bug;

import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that removes keys from a
 * a {@link org.apache.calcite.rel.core.Sort} if those keys are known to be
 * constant, or removes the entire Sort if all keys are constant.
 *
 * <p>Requires {@link RelCollationTraitDef}.
 */
@Value.Enclosing
public class SortRemoveConstantKeysRule
    extends RelRule<SortRemoveConstantKeysRule.Config>
    implements SubstitutionRule {

  /** Creates a SortRemoveConstantKeysRule. */
  protected SortRemoveConstantKeysRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    if (!Bug.CALCITE_6611_FIXED) {
      if (call.getPlanner().getRelTraitDefs()
          .contains(RelCollationTraitDef.INSTANCE)) {
        return;
      }
    }
    final Sort sort = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelNode input = sort.getInput();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(input);
    if (RelOptPredicateList.isEmpty(predicates)) {
      return;
    }

    final RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
    final List<RelFieldCollation> collationsList =
        sort.getCollation().getFieldCollations().stream()
            .filter(fc ->
                !predicates.constantMap.containsKey(
                    rexBuilder.makeInputRef(input, fc.getFieldIndex())))
            .collect(Collectors.toList());

    if (collationsList.size() == sort.collation.getFieldCollations().size()) {
      return;
    }

    // No active collations. Remove the sort completely
    if (collationsList.isEmpty() && sort.offset == null && sort.fetch == null) {
      call.transformTo(input);
      call.getPlanner().prune(sort);
      return;
    }

    final RelCollation collation = RelCollations.of(collationsList);
    final Sort result =
        sort.copy(
            sort.getTraitSet().replaceIf(RelCollationTraitDef.INSTANCE, () -> collation),
            input,
            collation);
    call.transformTo(result);
    call.getPlanner().prune(sort);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortRemoveConstantKeysRule.Config.of()
        .withOperandSupplier(b -> b.operand(Sort.class).anyInputs());

    @Override default SortRemoveConstantKeysRule toRule() {
      return new SortRemoveConstantKeysRule(this);
    }
  }
}
