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
package org.apache.calcite.rel.rules.dm;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexUtil;

import org.immutables.value.Value;

/**
 * Rule that flatten the nested correlation in filter condition.
 */
@Value.Enclosing
public class FilterNestedCorrelationTransposeRule
    extends RelRule<FilterNestedCorrelationTransposeRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /**
   * Creates an FilterNestedCorrelationTransposeRule.
   */
  protected FilterNestedCorrelationTransposeRule(FilterNestedCorrelationTransposeRule.Config config) {
    super(config);
  }

  public void setExtension(RuleMatchExtension extension) {
    this.extension = extension;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    extension.execute(call);
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    FilterNestedCorrelationTransposeRule.Config DEFAULT =
        ImmutableFilterNestedCorrelationTransposeRule.Config.of()
            .withOperandSupplier(b -> b.operand(Filter.class)
                .predicate(filter -> !filter.getVariablesSet().isEmpty()
                    && RexUtil.SubQueryFinder.containsSubQuery(filter))
                .anyInputs()).as(FilterNestedCorrelationTransposeRule.Config.class);

    @Override default FilterNestedCorrelationTransposeRule toRule() {
      return new FilterNestedCorrelationTransposeRule(this);
    }
  }
}
