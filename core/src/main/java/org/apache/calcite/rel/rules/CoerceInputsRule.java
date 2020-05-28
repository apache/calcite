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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * CoerceInputsRule pre-casts inputs to a particular type. This can be used to
 * assist operator implementations which impose requirements on their input
 * types.
 *
 * @see CoreRules#COERCE_INPUTS
 */
public class CoerceInputsRule
    extends RelRule<CoerceInputsRule.Config>
    implements TransformationRule {
  //~ Constructors -----------------------------------------------------------

  /** Creates a CoerceInputsRule. */
  protected CoerceInputsRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public CoerceInputsRule(
      Class<? extends RelNode> consumerRelClass,
      boolean coerceNames) {
    this(Config.DEFAULT
        .withCoerceNames(coerceNames)
        .withOperandFor(consumerRelClass));
  }

  @Deprecated // to be removed before 2.0
  public CoerceInputsRule(Class<? extends RelNode> consumerRelClass,
      boolean coerceNames, RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withCoerceNames(coerceNames)
        .withConsumerRelClass(consumerRelClass));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public @Nullable Convention getOutConvention() {
    return Convention.NONE;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    RelNode consumerRel = call.rel(0);
    if (consumerRel.getClass() != config.consumerRelClass()) {
      // require exact match on type
      return;
    }
    List<RelNode> inputs = consumerRel.getInputs();
    List<RelNode> newInputs = new ArrayList<>(inputs);
    boolean coerce = false;
    for (int i = 0; i < inputs.size(); ++i) {
      RelDataType expectedType = consumerRel.getExpectedInputRowType(i);
      RelNode input = inputs.get(i);
      RelNode newInput =
          RelOptUtil.createCastRel(
              input,
              expectedType,
              config.isCoerceNames());
      if (newInput != input) {
        newInputs.set(i, newInput);
        coerce = true;
      }
      assert RelOptUtil.areRowTypesEqual(
          newInputs.get(i).getRowType(),
          expectedType,
          config.isCoerceNames());
    }
    if (!coerce) {
      return;
    }
    RelNode newConsumerRel =
        consumerRel.copy(
            consumerRel.getTraitSet(),
            newInputs);
    call.transformTo(newConsumerRel);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withCoerceNames(false)
        .withOperandFor(RelNode.class);

    @Override default CoerceInputsRule toRule() {
      return new CoerceInputsRule(this);
    }

    /** Whether to coerce names. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isCoerceNames();

    /** Sets {@link #isCoerceNames()}. */
    Config withCoerceNames(boolean coerceNames);

    /** Class of {@link RelNode} to coerce to. */
    @ImmutableBeans.Property
    Class<? extends RelNode> consumerRelClass();

    /** Sets {@link #consumerRelClass()}. */
    Config withConsumerRelClass(Class<? extends RelNode> relClass);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends RelNode> consumerRelClass) {
      return withConsumerRelClass(consumerRelClass)
          .withOperandSupplier(b -> b.operand(consumerRelClass).anyInputs())
          .withDescription("CoerceInputsRule:" + consumerRelClass.getName())
          .as(Config.class);
    }
  }
}
