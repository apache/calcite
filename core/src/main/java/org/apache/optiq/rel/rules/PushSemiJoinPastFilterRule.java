/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.rel.rules;

import org.apache.optiq.rel.*;
import org.apache.optiq.relopt.*;

/**
 * PushSemiJoinPastFilterRule implements the rule for pushing semijoins down in
 * a tree past a filter in order to trigger other rules that will convert
 * semijoins. SemiJoinRel(FilterRel(X), Y) &rarr; FilterRel(SemiJoinRel(X, Y))
 */
public class PushSemiJoinPastFilterRule extends RelOptRule {
  public static final PushSemiJoinPastFilterRule INSTANCE =
      new PushSemiJoinPastFilterRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushSemiJoinPastFilterRule.
   */
  private PushSemiJoinPastFilterRule() {
    super(
        operand(
            SemiJoinRel.class,
            some(operand(FilterRel.class, any()))));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    SemiJoinRel semiJoin = call.rel(0);
    FilterRel filter = call.rel(1);

    RelNode newSemiJoin =
        new SemiJoinRel(
            semiJoin.getCluster(),
            filter.getChild(),
            semiJoin.getRight(),
            semiJoin.getCondition(),
            semiJoin.getLeftKeys(),
            semiJoin.getRightKeys());

    RelNode newFilter =
        CalcRel.createFilter(
            newSemiJoin,
            filter.getCondition());

    call.transformTo(newFilter);
  }
}

// End PushSemiJoinPastFilterRule.java
