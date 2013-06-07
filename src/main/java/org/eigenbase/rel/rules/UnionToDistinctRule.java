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
 * <code>UnionToDistinctRule</code> translates a distinct {@link UnionRel}
 * (<code>all</code> = <code>false</code>) into an {@link AggregateRel} on top
 * of a non-distinct {@link UnionRel} (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule
    extends RelOptRule
{
    public static final UnionToDistinctRule instance =
        new UnionToDistinctRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a UnionToDistinctRule.
     */
    private UnionToDistinctRule()
    {
        super(any(UnionRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        UnionRel union = call.rel(0);
        if (union.all) {
            return; // nothing to do
        }
        UnionRel unionAll =
            new UnionRel(
                union.getCluster(),
                union.getInputs(),
                true);
        call.transformTo(RelOptUtil.createDistinctRel(unionAll));
    }
}

// End UnionToDistinctRule.java
