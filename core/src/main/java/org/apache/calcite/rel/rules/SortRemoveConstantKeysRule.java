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
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that removes keys from a
 * {@link org.apache.calcite.rel.core.Sort} if those keys are known to be
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
      final RelTraitSet traits = sort.getInput().getTraitSet()
          .replaceIfs(RelCollationTraitDef.INSTANCE,
              () -> sort.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE));

      // We won't copy the RelTraitSet for every node in the RelSubset,
      // so stripped is probably a good choice.
      RelNode stripped = input.stripped();
      call.transformTo(
          convert(stripped.copy(traits, stripped.getInputs()),
              traits.replaceIf(ConventionTraitDef.INSTANCE, sort::getConvention)));
      call.getPlanner().prune(sort);
      return;
    }

    final RelCollation collation = RelCollations.of(collationsList);
    RelCollation sortCollation = sort.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

    final Sort result =
        sort.copy(
            sort.getTraitSet().
                replaceIfs(RelCollationTraitDef.INSTANCE,
                    () -> ImmutableList.of(collation,
                        requireNonNull(sortCollation, "sortCollation"))),
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
