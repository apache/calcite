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
 * Planner rule which merges a {@link CalcRel} onto a {@link CalcRel}. The
 * resulting {@link CalcRel} has the same project list as the upper {@link
 * CalcRel}, but expressed in terms of the lower {@link CalcRel}'s inputs.
 */
public class MergeCalcRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final MergeCalcRule instance = new MergeCalcRule();

    //~ Constructors -----------------------------------------------------------

    private MergeCalcRule()
    {
        super(
            some(
                CalcRel.class, any(CalcRel.class)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final CalcRel topCalc = call.rel(0);
        final CalcRel bottomCalc = call.rel(1);

        // Don't merge a calc which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter.
        RexProgram program = topCalc.getProgram();
        if (RexOver.containsOver(program)) {
            return;
        }

        // Merge the programs together.

        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topCalc.getProgram(),
                bottomCalc.getProgram(),
                topCalc.getCluster().getRexBuilder());
        final CalcRel newCalc =
            new CalcRel(
                bottomCalc.getCluster(),
                bottomCalc.getTraitSet(),
                bottomCalc.getChild(),
                topCalc.getRowType(),
                mergedProgram,
                Collections.<RelCollation>emptyList());

        if (newCalc.getDigest().equals(bottomCalc.getDigest())) {
            // newCalc is equivalent to bottomCalc, which means that topCalc
            // must be trivial. Take it out of the game.
            call.getPlanner().setImportance(topCalc, 0.0);
        }

        call.transformTo(newCalc);
    }
}

// End MergeCalcRule.java
