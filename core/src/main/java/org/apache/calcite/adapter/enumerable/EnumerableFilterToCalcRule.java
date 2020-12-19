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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/** Variant of {@link org.apache.calcite.rel.rules.FilterToCalcRule} for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 *
 * @see EnumerableRules#ENUMERABLE_FILTER_TO_CALC_RULE */
public class EnumerableFilterToCalcRule
    extends RelRule<EnumerableFilterToCalcRule.Config> {
  /** Creates an EnumerableFilterToCalcRule. */
  protected EnumerableFilterToCalcRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public EnumerableFilterToCalcRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final EnumerableFilter filter = call.rel(0);
    final RelNode input = filter.getInput();

    // Create a program containing a filter.
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RelDataType inputRowType = input.getRowType();
    final RexProgramBuilder programBuilder =
        new RexProgramBuilder(inputRowType, rexBuilder);
    programBuilder.addIdentity();
    programBuilder.addCondition(filter.getCondition());
    final RexProgram program = programBuilder.getProgram();

    final EnumerableCalc calc = EnumerableCalc.create(input, program);
    call.transformTo(calc);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withOperandSupplier(b ->
            b.operand(EnumerableFilter.class).anyInputs())
        .as(Config.class);

    @Override default EnumerableFilterToCalcRule toRule() {
      return new EnumerableFilterToCalcRule(this);
    }
  }
}
