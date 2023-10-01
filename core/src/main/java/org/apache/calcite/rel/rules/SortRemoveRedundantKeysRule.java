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
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import org.immutables.value.Value;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Planner rule that removes keys from a
 * a {@link org.apache.calcite.rel.core.Sort} if those keys functionally depend on
 * other sort field.
 *
 * <p>Requires {@link org.apache.calcite.rel.metadata.RelMdFunctionalDependency}.
 */
@Value.Enclosing
public class SortRemoveRedundantKeysRule extends RelRule<SortRemoveRedundantKeysRule.Config> {

  /**
   * Creates a SortRemoveRedundantRule.
   */
  protected SortRemoveRedundantKeysRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final RelNode input = sort.getInput();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelCollation collation = sort.getCollation();
    final ImmutableIntList keys = collation.getKeys();
    if (keys.isEmpty()) {
      return;
    }
    ImmutableBitSet sortKeySet = ImmutableBitSet.of(keys);
    final Set<Integer> needRemoveKeys = new HashSet<>();
    // Remove redundant sort field by functional dependency
    for (int keyIndex = keys.size() - 1; keyIndex >= 0; keyIndex--) {
      Integer key = keys.get(keyIndex);
      final ImmutableBitSet remainingSortKeySet = sortKeySet.except(ImmutableBitSet.of(key));
      if (Boolean.TRUE.equals(mq.functionallyDetermine(input, remainingSortKeySet, key))) {
        needRemoveKeys.add(key);
        sortKeySet = remainingSortKeySet;
      }
    }
    if (needRemoveKeys.isEmpty()) {
      // No functional dependency, bail out.
      return;
    }
    List<RelFieldCollation> remainingFieldCollationList = collation.getFieldCollations()
        .stream()
        .filter(fieldCollation -> !needRemoveKeys.contains(fieldCollation.getFieldIndex()))
        .collect(Collectors.toList());
    final Sort result =
        sort.copy(sort.getTraitSet(), input, RelCollations.of(remainingFieldCollationList));
    call.transformTo(result);
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    SortRemoveRedundantKeysRule.Config DEFAULT = ImmutableSortRemoveRedundantKeysRule.Config.of()
        .withOperandSupplier(b -> b.operand(Sort.class).anyInputs());

    @Override default SortRemoveRedundantKeysRule toRule() {
      return new SortRemoveRedundantKeysRule(this);
    }
  }
}
