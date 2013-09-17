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
import org.eigenbase.reltype.RelDataTypeField;


/**
 * Rule to convert an {@link JoinRel inner join} to a {@link FilterRel filter}
 * on top of a {@link JoinRel cartesian inner join}.
 *
 * <p>One benefit of this transformation is that after it, the join condition
 * can be combined with conditions and expressions above the join. It also makes
 * the <code>FennelCartesianJoinRule</code> applicable.
 */
public final class ExtractJoinFilterRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton.
     */
    public static final ExtractJoinFilterRule instance =
        new ExtractJoinFilterRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an ExtractJoinFilterRule.
     */
    private ExtractJoinFilterRule()
    {
        super(any(JoinRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = call.rel(0);

        if (joinRel.getJoinType() != JoinRelType.INNER) {
            return;
        }

        if (joinRel.getCondition().isAlwaysTrue()) {
            return;
        }

        if (!joinRel.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return;
        }

        // NOTE jvs 14-Mar-2006:  See SwapJoinRule for why we
        // preserve attribute semiJoinDone here.

        RelNode cartesianJoinRel =
            new JoinRel(
                joinRel.getCluster(),
                joinRel.getLeft(),
                joinRel.getRight(),
                joinRel.getCluster().getRexBuilder().makeLiteral(true),
                joinRel.getJoinType(),
                Collections.<String>emptySet(),
                joinRel.isSemiJoinDone(),
                Collections.<RelDataTypeField>emptyList());

        RelNode filterRel =
            CalcRel.createFilter(
                cartesianJoinRel,
                joinRel.getCondition());

        call.transformTo(filterRel);
    }
}

// End ExtractJoinFilterRule.java
