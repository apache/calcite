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
 * PushFilterIntoMultiJoinRule implements the rule for pushing a {@link
 * FilterRel} into a {@link MultiJoinRel}.
 */
public class PushFilterIntoMultiJoinRule extends RelOptRule {
  public static final PushFilterIntoMultiJoinRule INSTANCE =
      new PushFilterIntoMultiJoinRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushFilterIntoMultiJoinRule.
   */
  private PushFilterIntoMultiJoinRule() {
    super(
        operand(
            FilterRel.class,
            operand(MultiJoinRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    FilterRel filterRel = call.rel(0);
    MultiJoinRel multiJoinRel = call.rel(1);

    MultiJoinRel newMultiJoinRel =
        new MultiJoinRel(
            multiJoinRel.getCluster(),
            multiJoinRel.getInputs(),
            multiJoinRel.getJoinFilter(),
            multiJoinRel.getRowType(),
            multiJoinRel.isFullOuterJoin(),
            multiJoinRel.getOuterJoinConditions(),
            multiJoinRel.getJoinTypes(),
            multiJoinRel.getProjFields(),
            multiJoinRel.getJoinFieldRefCountsMap(),
            filterRel.getCondition());

    call.transformTo(newMultiJoinRel);
  }
}

// End PushFilterIntoMultiJoinRule.java
