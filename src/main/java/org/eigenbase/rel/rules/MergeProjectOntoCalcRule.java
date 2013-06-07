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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * Planner rule which merges a {@link ProjectRel} and a {@link CalcRel}. The
 * resulting {@link CalcRel} has the same project list as the original {@link
 * ProjectRel}, but expressed in terms of the original {@link CalcRel}'s inputs.
 *
 * @see MergeFilterOntoCalcRule
 */
public class MergeProjectOntoCalcRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final MergeProjectOntoCalcRule instance =
        new MergeProjectOntoCalcRule();

    //~ Constructors -----------------------------------------------------------

    private MergeProjectOntoCalcRule()
    {
        super(
            some(
                ProjectRel.class, any(CalcRel.class)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final ProjectRel project = call.rel(0);
        final CalcRel calc = call.rel(1);

        // Don't merge a project which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter. Transform the project into an identical calc,
        // which we'll have chance to merge later, after the over is
        // expanded.
        RexProgram program =
            RexProgram.create(
                calc.getRowType(),
                project.getProjectExps(),
                null,
                project.getRowType(),
                project.getCluster().getRexBuilder());
        if (RexOver.containsOver(program)) {
            CalcRel projectAsCalc =
                new CalcRel(
                    project.getCluster(),
                    project.getTraitSet(),
                    calc,
                    project.getRowType(),
                    program,
                    Collections.<RelCollation>emptyList());
            call.transformTo(projectAsCalc);
            return;
        }

        // Create a program containing the project node's expressions.
        final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        final RexProgramBuilder progBuilder =
            new RexProgramBuilder(
                calc.getRowType(),
                rexBuilder);
        final RelDataTypeField [] fields = project.getRowType().getFields();
        for (int i = 0; i < project.getProjectExps().length; i++) {
            progBuilder.addProject(
                project.getProjectExps()[i],
                fields[i].getName());
        }
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
                project.getRowType(),
                mergedProgram,
                Collections.<RelCollation>emptyList());
        call.transformTo(newCalc);
    }
}

// End MergeProjectOntoCalcRule.java
