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


/**
 * CombineUnionsRule implements the rule for combining two non-distinct {@link
 * UnionRel}s into a single {@link UnionRel}.
 */
public class CombineUnionsRule
    extends RelOptRule
{
    public static final CombineUnionsRule instance =
        new CombineUnionsRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a CombineUnionsRule.
     */
    private CombineUnionsRule()
    {
        super(
            some(
                UnionRel.class, any(RelNode.class), any(RelNode.class)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        UnionRel topUnionRel = call.rel(0);
        UnionRel bottomUnionRel;

        // We want to combine the UnionRel that's in the second input first.
        // Hence, that's why the rule pattern matches on generic RelNodes
        // rather than explicit UnionRels.  By doing so, and firing this rule
        // in a bottom-up order, it allows us to only specify a single
        // pattern for this rule.
        if (call.rel(2) instanceof UnionRel) {
            bottomUnionRel = call.rel(2);
        } else if (call.rel(1) instanceof UnionRel) {
            bottomUnionRel = call.rel(1);
        } else {
            return;
        }

        // If distincts haven't been removed yet, defer invoking this rule
        if (!topUnionRel.all || !bottomUnionRel.all) {
            return;
        }

        // Combine the inputs from the bottom union with the other inputs from
        // the top union
        List<RelNode> unionInputs = new ArrayList<RelNode>();
        if (call.rel(2) instanceof UnionRel) {
            assert topUnionRel.getInputs().size() == 2;
            unionInputs.add(topUnionRel.getInput(0));
            unionInputs.addAll(bottomUnionRel.getInputs());
        } else {
            unionInputs.addAll(bottomUnionRel.getInputs());
            unionInputs.addAll(
                topUnionRel.getInputs().subList(
                    1, topUnionRel.getInputs().size()));
        }
        assert unionInputs.size()
               == bottomUnionRel.getInputs().size()
                  + topUnionRel.getInputs().size()
                  - 1;
        UnionRel newUnionRel =
            new UnionRel(
                topUnionRel.getCluster(),
                unionInputs,
                true);

        call.transformTo(newUnionRel);
    }
}

// End CombineUnionsRule.java
