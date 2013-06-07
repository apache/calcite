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

import java.util.ArrayList;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * PushFilterPastSetOpRule implements the rule for pushing a {@link FilterRel}
 * past a {@link SetOpRel}.
 */
public class PushFilterPastSetOpRule
    extends RelOptRule
{
    public static final PushFilterPastSetOpRule instance =
        new PushFilterPastSetOpRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushFilterPastSetOpRule.
     */
    private PushFilterPastSetOpRule()
    {
        super(
            some(
                FilterRel.class, any(SetOpRel.class)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel = call.rel(0);
        SetOpRel setOpRel = call.rel(1);

        RelOptCluster cluster = setOpRel.getCluster();
        RexNode condition = filterRel.getCondition();

        // create filters on top of each setop child, modifying the filter
        // condition to reference each setop child
        RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        RelDataTypeField [] origFields = setOpRel.getRowType().getFields();
        int [] adjustments = new int[origFields.length];
        List<RelNode> newSetOpInputs = new ArrayList<RelNode>();
        for (RelNode input : setOpRel.getInputs()) {
            RexNode newCondition =
                condition.accept(
                    new RelOptUtil.RexInputConverter(
                        rexBuilder,
                        origFields,
                        input.getRowType().getFields(),
                        adjustments));
            newSetOpInputs.add(
                new FilterRel(cluster, input, newCondition));
        }

        // create a new setop whose children are the filters created above
        SetOpRel newSetOpRel =
            setOpRel.copy(setOpRel.getTraitSet(), newSetOpInputs);

        call.transformTo(newSetOpRel);
    }
}

// End PushFilterPastSetOpRule.java
