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
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that converts a {@link Calc}
 * to a {@link org.apache.calcite.rel.core.Project}
 * and {@link Filter}.
 *
 * <p>Not enabled by default, as it works against the usual flow, which is to
 * convert {@code Project} and {@code Filter} to {@code Calc}. But useful for
 * specific tasks, such as optimizing before calling an
 * {@link org.apache.calcite.interpreter.Interpreter}.
 *
 * @see CoreRules#CALC_SPLIT
 */
public class CalcSplitRule extends RelRule<CalcSplitRule.Config>
    implements TransformationRule {

  /** Creates a CalcSplitRule. */
  protected CalcSplitRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public CalcSplitRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Calc calc = call.rel(0);
    final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter =
        calc.getProgram().split();
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(calc.getInput());
    relBuilder.filter(projectFilter.right);
    relBuilder.project(projectFilter.left, calc.getRowType().getFieldNames());
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withOperandSupplier(b -> b.operand(Calc.class).anyInputs())
        .as(Config.class);

    @Override default CalcSplitRule toRule() {
      return new CalcSplitRule(this);
    }
  }
}
