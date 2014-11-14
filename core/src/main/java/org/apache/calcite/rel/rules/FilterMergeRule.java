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

/**
 * MergeFilterRule implements the rule for combining two {@link FilterRel}s
 */
public class MergeFilterRule extends RelOptRule {
  public static final MergeFilterRule INSTANCE =
      new MergeFilterRule(RelFactories.DEFAULT_FILTER_FACTORY);

  private final RelFactories.FilterFactory filterFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a MergeFilterRule.
   */
  public MergeFilterRule(RelFactories.FilterFactory filterFactory) {
    super(
        operand(FilterRelBase.class,
            operand(FilterRelBase.class, any())));
    this.filterFactory = filterFactory;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final FilterRelBase topFilter = call.rel(0);
    final FilterRelBase bottomFilter = call.rel(1);

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

    FilterRelBase newFilterRel =
        (FilterRelBase) filterFactory.createFilter(
            bottomFilter.getChild(),
            RexUtil.flatten(rexBuilder, newCondition));

    call.transformTo(newFilterRel);
  }

  /**
   * Creates a RexProgram corresponding to a FilterRel
   *
   * @param filterRel the FilterRel
   * @return created RexProgram
   */
  private RexProgram createProgram(FilterRelBase filterRel) {
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
