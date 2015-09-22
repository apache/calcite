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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that combines two
 * {@link org.apache.calcite.rel.logical.LogicalFilter}s.
 */
public class FilterMergeRule extends RelOptRule {
  public static final FilterMergeRule INSTANCE =
      new FilterMergeRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterMergeRule.
   */
  public FilterMergeRule(RelBuilderFactory relBuilderFactory) {
    super(
        operand(Filter.class,
            operand(Filter.class, any())),
        relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public FilterMergeRule(RelFactories.FilterFactory filterFactory) {
    this(RelBuilder.proto(Contexts.of(filterFactory)));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Filter topFilter = call.rel(0);
    final Filter bottomFilter = call.rel(1);

    // use RexPrograms to merge the two FilterRels into a single program
    // so we can convert the two LogicalFilter conditions to directly
    // reference the bottom LogicalFilter's child
    RexBuilder rexBuilder = topFilter.getCluster().getRexBuilder();
    RexProgram bottomProgram = createProgram(bottomFilter);
    RexProgram topProgram = createProgram(topFilter);

    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    RexNode newCondition =
        mergedProgram.expandLocalRef(
            mergedProgram.getCondition());

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(bottomFilter.getInput())
        .filter(newCondition);

    call.transformTo(relBuilder.build());
  }

  /**
   * Creates a RexProgram corresponding to a LogicalFilter
   *
   * @param filterRel the LogicalFilter
   * @return created RexProgram
   */
  private RexProgram createProgram(Filter filterRel) {
    RexProgramBuilder programBuilder =
        new RexProgramBuilder(
            filterRel.getRowType(),
            filterRel.getCluster().getRexBuilder());
    programBuilder.addIdentity();
    programBuilder.addCondition(filterRel.getCondition());
    return programBuilder.getProgram();
  }
}

// End FilterMergeRule.java
