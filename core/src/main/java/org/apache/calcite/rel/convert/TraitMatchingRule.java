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
package org.apache.calcite.rel.convert;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * TraitMatchingRule adapts a converter rule, restricting it to fire only when
 * its input already matches the expected output trait. This can be used with
 * {@link org.apache.calcite.plan.hep.HepPlanner} in cases where alternate
 * implementations are available and it is desirable to minimize converters.
 */
public class TraitMatchingRule extends RelRule<TraitMatchingRule.Config> {
  /**
   * Creates a configuration for a TraitMatchingRule.
   *
   * @param converterRule     Rule to be restricted; rule must take a single
   *                          operand expecting a single input
   * @param relBuilderFactory Builder for relational expressions
   */
  public static Config config(ConverterRule converterRule,
      RelBuilderFactory relBuilderFactory) {
    final RelOptRuleOperand operand = converterRule.getOperand();
    assert operand.childPolicy == RelOptRuleOperandChildPolicy.ANY;
    return Config.EMPTY.withRelBuilderFactory(relBuilderFactory)
        .withDescription("TraitMatchingRule: " + converterRule)
        .withOperandSupplier(b0 ->
            b0.operand(operand.getMatchedClass()).oneInput(b1 ->
                b1.operand(RelNode.class).anyInputs()))
        .as(Config.class)
        .withConverterRule(converterRule);
  }

  //~ Constructors -----------------------------------------------------------

  /** Creates a TraitMatchingRule. */
  protected TraitMatchingRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public TraitMatchingRule(ConverterRule converterRule) {
    this(config(converterRule, RelFactories.LOGICAL_BUILDER));
  }

  @Deprecated // to be removed before 2.0
  public TraitMatchingRule(ConverterRule converterRule,
      RelBuilderFactory relBuilderFactory) {
    this(config(converterRule, relBuilderFactory));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public @Nullable Convention getOutConvention() {
    return config.converterRule().getOutConvention();
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelNode input = call.rel(1);
    final ConverterRule converterRule = config.converterRule();
    if (input.getTraitSet().contains(converterRule.getOutTrait())) {
      converterRule.onMatch(call);
    }
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    @Override default TraitMatchingRule toRule() {
      return new TraitMatchingRule(this);
    }

    /** Returns the rule to be restricted; rule must take a single
     * operand expecting a single input. */
    @ImmutableBeans.Property
    ConverterRule converterRule();

    /** Sets {@link #converterRule()}. */
    Config withConverterRule(ConverterRule converterRule);
  }
}
