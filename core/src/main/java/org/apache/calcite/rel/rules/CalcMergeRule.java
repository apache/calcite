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
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

/**
 * Planner rule that merges a
 * {@link org.apache.calcite.rel.core.Calc} onto a
 * {@link org.apache.calcite.rel.core.Calc}.
 *
 * <p>The resulting {@link org.apache.calcite.rel.core.Calc} has the
 * same project list as the upper
 * {@link org.apache.calcite.rel.core.Calc}, but expressed in terms of
 * the lower {@link org.apache.calcite.rel.core.Calc}'s inputs.
 *
 * @see CoreRules#CALC_MERGE
 */
@Value.Enclosing
public class CalcMergeRule extends RelRule<CalcMergeRule.Config>
    implements TransformationRule {

  /** Creates a CalcMergeRule. */
  protected CalcMergeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public CalcMergeRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Calc topCalc = call.rel(0);
    final Calc bottomCalc = call.rel(1);

    // Don't merge a calc which contains windowed aggregates onto a
    // calc. That would effectively be pushing a windowed aggregate down
    // through a filter.
    RexProgram topProgram = topCalc.getProgram();
    if (RexOver.containsOver(topProgram)) {
      return;
    }

    // Merge the programs together.

    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topCalc.getProgram(),
            bottomCalc.getProgram(),
            topCalc.getCluster().getRexBuilder());
    assert mergedProgram.getOutputRowType()
        == topProgram.getOutputRowType();
    final Calc newCalc =
        topCalc.copy(
            topCalc.getTraitSet(),
            bottomCalc.getInput(),
            mergedProgram);

    if (newCalc.getDigest().equals(bottomCalc.getDigest())
        && newCalc.getRowType().equals(bottomCalc.getRowType())) {
      // newCalc is equivalent to bottomCalc, which means that topCalc
      // must be trivial. Take it out of the game.
      call.getPlanner().prune(topCalc);
    }

    call.transformTo(newCalc);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCalcMergeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Calc.class).oneInput(b1 ->
                b1.operand(Calc.class).anyInputs()))
        .as(Config.class);

    @Override default CalcMergeRule toRule() {
      return new CalcMergeRule(this);
    }
  }
}
