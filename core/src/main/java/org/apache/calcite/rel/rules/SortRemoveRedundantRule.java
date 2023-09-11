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
import org.apache.calcite.rel.core.Sort;

import org.immutables.value.Value;

/**
 * Planner rule that removes
 * the redundant {@link org.apache.calcite.rel.core.Sort} if its input
 * max row number is less than or equal to one.
 *
 * <p> For example:
 * <blockquote><pre>{@code
 *  select max(totalprice) from orders order by 1}
 *  </pre></blockquote>
 *
 * <p> could be converted to
 * <blockquote><pre>{@code
 *  select max(totalprice) from orders}
 *  </pre></blockquote>
 *
 * @see CoreRules#SORT_REMOVE_REDUNDANT
 */
@Value.Enclosing
public class SortRemoveRedundantRule
    extends RelRule<SortRemoveRedundantRule.Config>
    implements TransformationRule {
  protected SortRemoveRedundantRule(final SortRemoveRedundantRule.Config config) {
    super(config);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    if (sort.offset != null || sort.fetch != null) {
      // Don't remove sort if it has explicit OFFSET and LIMIT
      return;
    }

    // Get the max row count for sort's input RelNode.
    final Double maxRowCount = call.getMetadataQuery().getMaxRowCount(sort.getInput());
    // If the max row count is not null and less than or equal to 1,
    // then we could remove the sort.
    if (maxRowCount != null && maxRowCount <= 1D) {
      call.transformTo(sort.getInput());
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortRemoveRedundantRule.Config.of()
        .withOperandSupplier(b ->
            b.operand(Sort.class).anyInputs());

    @Override default SortRemoveRedundantRule toRule() {
      return new SortRemoveRedundantRule(this);
    }
  }
}
