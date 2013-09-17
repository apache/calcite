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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * Planner rule which merges a {@link FilterRel} and a {@link CalcRel}. The
 * result is a {@link CalcRel} whose filter condition is the logical AND of the
 * two.
 *
 * @see MergeFilterOntoCalcRule
 */
public class MergeFilterOntoCalcRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final MergeFilterOntoCalcRule instance =
        new MergeFilterOntoCalcRule();

    //~ Constructors -----------------------------------------------------------

    private MergeFilterOntoCalcRule()
    {
        super(
            some(
                FilterRel.class, any(CalcRel.class)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final FilterRel filter = call.rel(0);
        final CalcRel calc = call.rel(1);

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
        final CalcRel newCalc =
            new CalcRel(
                calc.getCluster(),
                calc.getTraitSet(),
                calc.getChild(),
                filter.getRowType(),
                mergedProgram,
                Collections.<RelCollation>emptyList());
        call.transformTo(newCalc);
    }
}

// End MergeFilterOntoCalcRule.java
