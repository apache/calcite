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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * Rule that transforms a {@link Filter} on top of a {@link Sort}
 * into a {@link Sort} on top of a {@link Filter}.
 */
@Value.Enclosing
public class FilterSortTransposeRule
    extends RelRule<FilterSortTransposeRule.Config>
    implements TransformationRule {
  protected FilterSortTransposeRule(FilterSortTransposeRule.Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Sort sort = call.rel(1);

    final RelBuilder builder = call.builder();
    final RexNode condition = filter.getCondition();
    RelNode newSort = builder.push(sort.getInput())
        .filter(condition)
        .sort(sort.getCollation())
        .build();

    call.transformTo(newSort);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFilterSortTransposeRule.Config.of()
        .withOperandSupplier(f ->
            f.operand(Filter.class)
                .oneInput(s ->
                    s.operand(Sort.class)
                        .predicate(RelOptUtil::isPureOrder)
                        .anyInputs()));

    @Override default FilterSortTransposeRule toRule() {
      return new FilterSortTransposeRule(this);
    }
  }
}
