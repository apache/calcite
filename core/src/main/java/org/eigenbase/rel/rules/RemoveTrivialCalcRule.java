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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * Rule which removes a trivial {@link CalcRel}.
 *
 * <p>A {@link CalcRel} is trivial if it projects its input fields in their
 * original order, and it does not filter.
 *
 * @see org.eigenbase.rel.rules.RemoveTrivialProjectRule
 */
public class RemoveTrivialCalcRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final RemoveTrivialCalcRule instance =
        new RemoveTrivialCalcRule();

    //~ Constructors -----------------------------------------------------------

    private RemoveTrivialCalcRule()
    {
        super(any(CalcRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = call.rel(0);
        RexProgram program = calc.getProgram();
        if (!program.isTrivial()) {
            return;
        }
        RelNode child = calc.getInput(0);
        child = call.getPlanner().register(child, calc);
        call.transformTo(
            convert(
                child,
                calc.getTraitSet()));
    }
}

// End RemoveTrivialCalcRule.java
