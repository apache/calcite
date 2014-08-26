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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule which converts a {@link FilterRel} to a {@link CalcRel}.
 *
 * <p>The rule does <em>NOT</em> fire if the child is a {@link FilterRel} or a
 * {@link ProjectRel} (we assume they they will be converted using {@link
 * FilterToCalcRule} or {@link ProjectToCalcRule}) or a {@link CalcRel}. This
 * {@link FilterRel} will eventually be converted by {@link
 * MergeFilterOntoCalcRule}.
 */
public class FilterToCalcRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final FilterToCalcRule INSTANCE = new FilterToCalcRule();

  //~ Constructors -----------------------------------------------------------

  private FilterToCalcRule() {
    super(operand(FilterRel.class, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final FilterRel filter = call.rel(0);
    final RelNode rel = filter.getChild();

    // Create a program containing a filter.
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RelDataType inputRowType = rel.getRowType();
    final RexProgramBuilder programBuilder =
        new RexProgramBuilder(inputRowType, rexBuilder);
    programBuilder.addIdentity();
    programBuilder.addCondition(filter.getCondition());
    final RexProgram program = programBuilder.getProgram();

    final CalcRel calc =
        new CalcRel(
            filter.getCluster(),
            filter.getTraitSet(),
            rel,
            inputRowType,
            program,
            ImmutableList.<RelCollation>of());
    call.transformTo(calc);
  }
}

// End FilterToCalcRule.java
