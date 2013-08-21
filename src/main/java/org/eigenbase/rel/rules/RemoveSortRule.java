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
 * Planner rule that removes a {@link SortRel} if its input is already sorted.
 * Requires {@link RelCollationTraitDef}.
 */
public class RemoveSortRule extends RelOptRule {
    public static final RemoveSortRule INSTANCE = new RemoveSortRule();

    private RemoveSortRule() {
        super(
            any(SortRel.class),
            "RemoveSortRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (!call.getPlanner().getRelTraitDefs()
            .contains(RelCollationTraitDef.INSTANCE))
        {
            // Collation is not an active trait.
            return;
        }
        final SortRel sort = call.rel(0);
        if (sort.offset != null || sort.fetch != null) {
            // Don't remove sort if would also remove OFFSET or LIMIT.
            return;
        }
        final RelCollation collation = sort.getCollation();
        final RelTraitSet traits = sort.getTraitSet().replace(collation);
        call.transformTo(convert(sort.getChild(), traits));
    }
}

// End RemoveSortRule.java
