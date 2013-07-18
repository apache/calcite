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
import org.eigenbase.util.*;

/**
 * Rule that slices the {@link CalcRel} into sections which contain windowed
 * agg functions and sections which do not.
 *
 * <p>The sections which contain windowed agg functions become instances of
 * {@link org.eigenbase.rel.WindowRel}. If the {@link CalcRel} does not contain any
 * windowed agg functions, does nothing.
 */
public class WindowedAggSplitterRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The {@link Glossary#SingletonPattern singleton} instance.
     */
    public static final WindowedAggSplitterRule INSTANCE =
        new WindowedAggSplitterRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a rule.
     */
    private WindowedAggSplitterRule()
    {
        super(any(CalcRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = call.rel(0);
        if (!RexOver.containsOver(calc.getProgram())) {
            return;
        }
        CalcRelSplitter transform = new WindowedAggRelSplitter(calc);
        RelNode newRel = transform.execute();
        call.transformTo(newRel);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Splitter which distinguishes between windowed aggregation expressions
     * (calls to {@link RexOver}) and ordinary expressions.
     */
    static class WindowedAggRelSplitter
        extends CalcRelSplitter
    {
        WindowedAggRelSplitter(CalcRel calc)
        {
            super(
                calc,
                new RelType[] {
                    new CalcRelSplitter.RelType("CalcRelType") {
                        protected boolean canImplement(RexFieldAccess field)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexDynamicParam param)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexLiteral literal)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexCall call)
                        {
                            return !(call instanceof RexOver);
                        }

                        protected RelNode makeRel(
                            RelOptCluster cluster,
                            RelTraitSet traits,
                            RelDataType rowType,
                            RelNode child,
                            RexProgram program)
                        {
                            assert !program.containsAggs();
                            return super.makeRel(
                                cluster,
                                traits,
                                rowType,
                                child,
                                program);
                        }
                    },
                    new CalcRelSplitter.RelType("WinAggRelType") {
                        protected boolean canImplement(RexFieldAccess field)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexDynamicParam param)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexLiteral literal)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexCall call)
                        {
                            return call instanceof RexOver;
                        }

                        protected boolean supportsCondition()
                        {
                            return false;
                        }

                        protected RelNode makeRel(
                            RelOptCluster cluster,
                            RelTraitSet traits,
                            RelDataType rowType,
                            RelNode child,
                            RexProgram program)
                        {
                            Util.permAssert(
                                program.getCondition() == null,
                                "WindowedAggregateRel cannot accept a condition");
                            return WindowRel.create(
                                cluster, traits, child, program, rowType);
                        }
                    }
                });
        }

        @Override
        protected List<Set<Integer>> getCohorts()
        {
            final Set<Integer> cohort = new LinkedHashSet<Integer>();
            final List<RexNode> exprList = program.getExprList();
            for (int i = 0; i < exprList.size(); i++) {
                RexNode expr = exprList.get(i);
                if (expr instanceof RexOver) {
                    cohort.add(i);
                }
            }
            if (cohort.isEmpty()) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(cohort);
            }
        }
    }
}

// End WindowedAggSplitterRule.java
