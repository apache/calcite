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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * CoerceInputsRule precasts inputs to a particular type. This can be used to
 * assist operator implementations which impose requirements on their input
 * types.
 */
public class CoerceInputsRule extends RelOptRule {
  //~ Instance fields --------------------------------------------------------

  private final Class consumerRelClass;

  private final boolean coerceNames;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs the rule.
   *
   * @param consumerRelClass the RelNode class which will consume the inputs
   * @param coerceNames      if true, coerce names and types; if false, coerce
   *                         type only
   */
  @Deprecated // to be removed before 2.0
  public CoerceInputsRule(
      Class<? extends RelNode> consumerRelClass,
      boolean coerceNames) {
    this(consumerRelClass, coerceNames, RelFactories.LOGICAL_BUILDER);
  }

  /**
   * Constructs the rule.
   *
   * @param consumerRelClass  the RelNode class which will consume the inputs
   * @param coerceNames       if true, coerce names and types; if false, coerce
   *                          type only
   * @param relBuilderFactory Builder for relational expressions
   */
  public CoerceInputsRule(
      Class<? extends RelNode> consumerRelClass,
      boolean coerceNames,
      RelBuilderFactory relBuilderFactory) {
    super(
        operand(consumerRelClass, any()),
        relBuilderFactory,
        "CoerceInputsRule:" + consumerRelClass.getName());
    this.consumerRelClass = consumerRelClass;
    this.coerceNames = coerceNames;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public Convention getOutConvention() {
    return Convention.NONE;
  }

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    RelNode consumerRel = call.rel(0);
    if (consumerRel.getClass() != consumerRelClass) {
      // require exact match on type
      return;
    }
    List<RelNode> inputs = consumerRel.getInputs();
    List<RelNode> newInputs = new ArrayList<RelNode>(inputs);
    boolean coerce = false;
    for (int i = 0; i < inputs.size(); ++i) {
      RelDataType expectedType = consumerRel.getExpectedInputRowType(i);
      RelNode input = inputs.get(i);
      RelNode newInput =
          RelOptUtil.createCastRel(
              input,
              expectedType,
              coerceNames);
      if (newInput != input) {
        newInputs.set(i, newInput);
        coerce = true;
      }
      assert RelOptUtil.areRowTypesEqual(
          newInputs.get(i).getRowType(),
          expectedType,
          coerceNames);
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
}

// End CoerceInputsRule.java
