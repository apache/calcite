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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that merges a
 * {@link org.apache.calcite.rel.logical.LogicalFilter} and a
 * {@link org.apache.calcite.rel.logical.LogicalCalc}. The
 * result is a {@link org.apache.calcite.rel.logical.LogicalCalc}
 * whose filter condition is the logical AND of the two.
 *
 * @see FilterMergeRule
 */
public class FilterCalcMergeRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final FilterCalcMergeRule INSTANCE =
      new FilterCalcMergeRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterCalcMergeRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public FilterCalcMergeRule(RelBuilderFactory relBuilderFactory) {
    super(
        operand(
            Filter.class,
            operand(LogicalCalc.class, any())),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final LogicalCalc calc = call.rel(1);

    // Don't merge a filter onto a calc which contains windowed aggregates.
    // That would effectively be pushing a multiset down through a filter.
    // We'll have chance to merge later, when the over is expanded.
    if (calc.getProgram().containsAggs()) {
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
    final LogicalCalc newCalc =
        LogicalCalc.create(calc.getInput(), mergedProgram);
    call.transformTo(newCalc);
  }
}

// End FilterCalcMergeRule.java
