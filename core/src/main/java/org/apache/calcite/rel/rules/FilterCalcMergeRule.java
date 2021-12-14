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
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

/**
 * Planner rule that merges a
 * {@link org.apache.calcite.rel.core.Filter} and a
 * {@link org.apache.calcite.rel.core.Calc}. The
 * result is a {@link org.apache.calcite.rel.core.Calc}
 * whose filter condition is the logical AND of the two.
 *
 * @see FilterMergeRule
 * @see ProjectCalcMergeRule
 * @see CoreRules#FILTER_CALC_MERGE
 */
@Value.Enclosing
public class FilterCalcMergeRule
    extends RelRule<FilterCalcMergeRule.Config>
    implements TransformationRule {

  /** Creates a FilterCalcMergeRule. */
  protected FilterCalcMergeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public FilterCalcMergeRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Calc calc = call.rel(1);

    // Don't merge a filter onto a calc which contains windowed aggregates.
    // That would effectively be pushing a multiset down through a filter.
    // We'll have chance to merge later, when the over is expanded.
    if (calc.containsOver()) {
      return;
    }

    // Create a program containing the filter.
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RexProgramBuilder progBuilder =
        new RexProgramBuilder(
            calc.getRowType(),
            rexBuilder);
    progBuilder.addIdentity();
    progBuilder.addCondition(filter.getCondition());
    RexProgram topProgram = progBuilder.getProgram();
    RexProgram bottomProgram = calc.getProgram();

    // Merge the programs together.
    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);
    final Calc newCalc =
        calc.copy(calc.getTraitSet(), calc.getInput(), mergedProgram);
    call.transformTo(newCalc);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFilterCalcMergeRule.Config.of()
        .withOperandFor(Filter.class, LogicalCalc.class);

    @Override default FilterCalcMergeRule toRule() {
      return new FilterCalcMergeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
        Class<? extends Calc> calcClass) {
      return withOperandSupplier(b0 ->
          b0.operand(filterClass).oneInput(b1 ->
              b1.operand(calcClass).anyInputs()))
          .as(Config.class);
    }
  }
}
