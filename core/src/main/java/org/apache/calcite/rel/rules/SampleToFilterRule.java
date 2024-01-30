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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * This rule rewrite {@link Sample} which is bernoulli to the {@link Filter}.
 *
 * <p> For example:
 * <blockquote><pre>{@code
 *    select deptno from "scott".dept tablesample bernoulli(50);
 * }</pre></blockquote>
 *
 * <p> will convert to:
 * <blockquote><pre>{@code
 *    select deptno from "scott".dept where rand() < 0.5;
 * }</pre></blockquote>
 *
 * <p> The sql:
 * <blockquote><pre>{@code
 *    select deptno from "scott".dept tablesample bernoulli(50) REPEATABLE(10);
 * }</pre></blockquote>
 *
 * <p> will convert to:
 * <blockquote><pre>{@code
 *    select deptno from "scott".dept where rand(10) < 0.5;
 * }</pre></blockquote>
 *
 * @see CoreRules#SAMPLE_TO_FILTER
 */
@Value.Enclosing
public class SampleToFilterRule
    extends RelRule<SampleToFilterRule.Config>
    implements TransformationRule {

  protected SampleToFilterRule(final SampleToFilterRule.Config config) {
    super(config);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    final Sample sample = call.rel(0);
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(sample.getInput());

    RexNode randFunc = sample.getSamplingParameters().isRepeatable()
        ? relBuilder.call(SqlStdOperatorTable.RAND,
        relBuilder.literal(sample.getSamplingParameters().getRepeatableSeed()))
        : relBuilder.call(SqlStdOperatorTable.RAND);

    relBuilder.filter(
        relBuilder.lessThan(randFunc,
            relBuilder.literal(sample.getSamplingParameters().sampleRate)));
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    SampleToFilterRule.Config DEFAULT = ImmutableSampleToFilterRule.Config.of()
        .withOperandFor(Sample.class);

    @Override default SampleToFilterRule toRule() {
      return new SampleToFilterRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default SampleToFilterRule.Config withOperandFor(Class<? extends Sample> sampleClass) {
      return withOperandSupplier(b ->
          b.operand(sampleClass)
              .predicate(sample -> sample.getSamplingParameters().isBernoulli()).anyInputs())
          .as(SampleToFilterRule.Config.class);
    }
  }
}
