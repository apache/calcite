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
import org.apache.optiq.rex.*;

/**
 * MergeFilterRule implements the rule for combining two {@link FilterRel}s
 */
public class MergeFilterRule extends RelOptRule {
  public static final MergeFilterRule INSTANCE = new MergeFilterRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a MergeFilterRule.
   */
  private MergeFilterRule() {
    super(
        operand(
            FilterRel.class,
            operand(FilterRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    FilterRel topFilter = call.rel(0);
    FilterRel bottomFilter = call.rel(1);

    // use RexPrograms to merge the two FilterRels into a single program
    // so we can convert the two FilterRel conditions to directly
    // reference the bottom FilterRel's child
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

    FilterRel newFilterRel =
        new FilterRel(
            topFilter.getCluster(),
            bottomFilter.getChild(),
            newCondition);

    call.transformTo(newFilterRel);
  }

  /**
   * Creates a RexProgram corresponding to a FilterRel
   *
   * @param filterRel the FilterRel
   * @return created RexProgram
   */
  private RexProgram createProgram(FilterRel filterRel) {
    RexProgramBuilder programBuilder =
        new RexProgramBuilder(
            filterRel.getRowType(),
            filterRel.getCluster().getRexBuilder());
    programBuilder.addIdentity();
    programBuilder.addCondition(filterRel.getCondition());
    return programBuilder.getProgram();
  }
}

// End MergeFilterRule.java
