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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that replaces {@code IS NOT DISTINCT FROM}
 * in a {@link org.apache.calcite.rel.logical.LogicalFilter}
 * with logically equivalent operations.
 *
 * @see org.apache.calcite.sql.fun.SqlStdOperatorTable#IS_NOT_DISTINCT_FROM
 */
public final class FilterRemoveIsNotDistinctFromRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final FilterRemoveIsNotDistinctFromRule INSTANCE =
      new FilterRemoveIsNotDistinctFromRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterRemoveIsNotDistinctFromRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public FilterRemoveIsNotDistinctFromRule(RelBuilderFactory relBuilderFactory) {
    super(operand(LogicalFilter.class, any()), relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    LogicalFilter oldFilter = call.rel(0);
    RexNode oldFilterCond = oldFilter.getCondition();

    if (RexUtil.findOperatorCall(
        SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        oldFilterCond)
        == null) {
      // no longer contains isNotDistinctFromOperator
      return;
    }

    // Now replace all the "a isNotDistinctFrom b"
    // with the RexNode given by RelOptUtil.isDistinctFrom() method

    RemoveIsNotDistinctFromRexShuttle rewriteShuttle =
        new RemoveIsNotDistinctFromRexShuttle(
            oldFilter.getCluster().getRexBuilder());

    final RelFactories.FilterFactory factory =
        RelFactories.DEFAULT_FILTER_FACTORY;
    RelNode newFilterRel =
        factory.createFilter(oldFilter.getInput(),
            oldFilterCond.accept(rewriteShuttle));

    call.transformTo(newFilterRel);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Shuttle that removes 'x IS NOT DISTINCT FROM y' and converts it
   * to 'CASE WHEN x IS NULL THEN y IS NULL WHEN y IS NULL THEN x IS
   * NULL ELSE x = y END'. */
  private class RemoveIsNotDistinctFromRexShuttle extends RexShuttle {
    RexBuilder rexBuilder;

    RemoveIsNotDistinctFromRexShuttle(
        RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    // override RexShuttle
    public RexNode visitCall(RexCall call) {
      RexNode newCall = super.visitCall(call);

      if (call.getOperator()
          == SqlStdOperatorTable.IS_NOT_DISTINCT_FROM) {
        RexCall tmpCall = (RexCall) newCall;
        newCall =
            RelOptUtil.isDistinctFrom(
                rexBuilder,
                tmpCall.operands.get(0),
                tmpCall.operands.get(1),
                true);
      }
      return newCall;
    }
  }
}

// End FilterRemoveIsNotDistinctFromRule.java
