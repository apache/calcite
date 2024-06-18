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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.Filter}
 * past a {@link org.apache.calcite.rel.core.Sample}.
 *
 * @see CoreRules#FILTER_SAMPLE_TRANSPOSE
 */
@Value.Enclosing
public class FilterSampleTransposeRule
    extends RelRule<FilterSampleTransposeRule.Config>
    implements TransformationRule {

  protected FilterSampleTransposeRule(Config config) {
    super(config);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Sample sample = call.rel(1);
    final RelBuilder relBuilder = call.builder();
    // Push filter condition past the Sample.
    relBuilder.push(sample.getInput());
    relBuilder.filter(filter.getCondition());
    // Build the sample on top of the filter.
    relBuilder.sample(sample.getSamplingParameters().isBernoulli(),
        sample.getSamplingParameters().sampleRate,
        sample.getSamplingParameters().getRepeatableSeed());
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFilterSampleTransposeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Filter.class).oneInput(b1 ->
                b1.operand(Sample.class).anyInputs()));

    @Override default FilterSampleTransposeRule toRule() {
      return new FilterSampleTransposeRule(this);
    }
  }
}
