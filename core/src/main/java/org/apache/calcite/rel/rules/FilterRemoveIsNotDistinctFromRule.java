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
package org.eigenbase.rel.rules;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;

/**
 * Rule to replace isNotDistinctFromOperator with logical equivalent conditions
 * in a {@link FilterRel}.
 */
public final class RemoveIsNotDistinctFromRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final RemoveIsNotDistinctFromRule INSTANCE =
      new RemoveIsNotDistinctFromRule();

  //~ Constructors -----------------------------------------------------------

  private RemoveIsNotDistinctFromRule() {
    super(operand(FilterRel.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    FilterRel oldFilterRel = call.rel(0);
    RexNode oldFilterCond = oldFilterRel.getCondition();

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
            oldFilterRel.getCluster().getRexBuilder());

    RelNode newFilterRel =
        RelOptUtil.createFilter(
            oldFilterRel.getChild(),
            oldFilterCond.accept(rewriteShuttle));

    call.transformTo(newFilterRel);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Shuttle that removes 'x IS NOT DISTINCT FROM y' and converts it
   * to 'CASE WHEN x IS NULL THEN y IS NULL WHEN y IS NULL THEN x IS
   * NULL ELSE x = y END'. */
  private class RemoveIsNotDistinctFromRexShuttle extends RexShuttle {
    RexBuilder rexBuilder;

    public RemoveIsNotDistinctFromRexShuttle(
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

// End RemoveIsNotDistinctFromRule.java
