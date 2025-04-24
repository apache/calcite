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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

/**
 * Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalFilter} to a
 * {@link org.apache.calcite.rel.logical.LogicalCalc}.
 *
 * <p>The rule does <em>NOT</em> fire if the child is a
 * {@link org.apache.calcite.rel.logical.LogicalFilter} or a
 * {@link org.apache.calcite.rel.logical.LogicalProject} (we assume that they
 * will be converted using {@link FilterToCalcRule} or
 * {@link ProjectToCalcRule}) or a
 * {@link org.apache.calcite.rel.logical.LogicalCalc}. This
 * {@link org.apache.calcite.rel.logical.LogicalFilter} will eventually be
 * converted by {@link FilterCalcMergeRule}.
 *
 * @see CoreRules#FILTER_TO_CALC
 */
@Value.Enclosing
public class FilterToCalcRule
    extends RelRule<FilterToCalcRule.Config>
    implements TransformationRule {

  /** Creates a FilterToCalcRule. */
  protected FilterToCalcRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public FilterToCalcRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final RelNode rel = filter.getInput();

    // Create a program containing a filter.
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RelDataType inputRowType = rel.getRowType();
    final RexProgramBuilder programBuilder =
        new RexProgramBuilder(inputRowType, rexBuilder);
    programBuilder.addIdentity();
    programBuilder.addCondition(filter.getCondition());
    final RexProgram program = programBuilder.getProgram();

    final LogicalCalc calc = LogicalCalc.create(rel, program);
    call.transformTo(calc);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFilterToCalcRule.Config.of()
        .withOperandSupplier(b ->
            b.operand(LogicalFilter.class)
                .predicate(filter ->
                    !RexUtil.SubQueryFinder.containsSubQuery(filter))
                .anyInputs());

    @Override default FilterToCalcRule toRule() {
      return new FilterToCalcRule(this);
    }
  }
}
