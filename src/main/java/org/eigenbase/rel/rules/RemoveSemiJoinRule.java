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

import org.eigenbase.relopt.*;


/**
 * RemoveSemiJoinRule implements the rule that removes semijoins from a join
 * tree if it turns out it's not possible to convert a SemiJoinRel to an indexed
 * scan on a join factor. Namely, if the join factor does not reduce to a single
 * table that can be scanned using an index. This rule should only be applied
 * after attempts have been made to convert SemiJoinRels.
 */
public class RemoveSemiJoinRule
    extends RelOptRule
{
    public static final RemoveSemiJoinRule instance =
        new RemoveSemiJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RemoveSemiJoinRule.
     */
    private RemoveSemiJoinRule()
    {
        super(any(SemiJoinRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        call.transformTo(call.rel(0).getInput(0));
    }
}

// End RemoveSemiJoinRule.java
