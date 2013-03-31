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


/**
 * Rule to remove an {@link AggregateRel} implementing DISTINCT if the
 * underlying relational expression is already distinct.
 */
public class RemoveDistinctRule
    extends RelOptRule
{
    public static final RemoveDistinctRule instance =
        new RemoveDistinctRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RemoveDistinctRule.
     */
    private RemoveDistinctRule()
    {
        // REVIEW jvs 14-Mar-2006: We have to explicitly mention the child here
        // to make sure the rule re-fires after the child changes (e.g. via
        // RemoveTrivialProjectRule), since that may change our information
        // about whether the child is distinct.  If we clean up the inference of
        // distinct to make it correct up-front, we can get rid of the reference
        // to the child here.
        super(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand(RelNode.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel distinct = (AggregateRel) call.rels[0];
        if (!distinct.isDistinct()) {
            return;
        }
        RelNode child = distinct.getChild();
        if (child.isDistinct()) {
            child = call.getPlanner().register(child, distinct);
            call.transformTo(
                convert(
                    child,
                    distinct.getTraitSet()));
        }
    }
}

// End RemoveDistinctRule.java
