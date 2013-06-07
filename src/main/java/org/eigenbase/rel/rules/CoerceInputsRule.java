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


/**
 * CoerceInputsRule precasts inputs to a particular type. This can be used to
 * assist operator implementations which impose requirements on their input
 * types.
 */
public class CoerceInputsRule
    extends RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    private final Class consumerRelClass;

    private final boolean coerceNames;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs the rule.
     *
     * @param consumerRelClass the RelNode class which will consume the inputs
     * @param coerceNames if true, coerce names and types; if false, coerce type
     * only
     */
    public CoerceInputsRule(
        Class<? extends RelNode> consumerRelClass,
        boolean coerceNames)
    {
        super(
            any(consumerRelClass),
            "CoerceInputsRule:" + consumerRelClass.getName());
        this.consumerRelClass = consumerRelClass;
        this.coerceNames = coerceNames;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public Convention getOutConvention()
    {
        return Convention.NONE;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        RelNode consumerRel = call.rel(0);
        if (consumerRel.getClass() != consumerRelClass) {
            // require exact match on type
            return;
        }
        List<RelNode> inputs = consumerRel.getInputs();
        List<RelNode> newInputs = new ArrayList<RelNode>(inputs);
        boolean coerce = false;
        for (int i = 0; i < inputs.size(); ++i) {
            RelDataType expectedType = consumerRel.getExpectedInputRowType(i);
            RelNode input = inputs.get(i);
            RelNode newInput =
                RelOptUtil.createCastRel(
                    input,
                    expectedType,
                    coerceNames);
            if (newInput != input) {
                newInputs.set(i, newInput);
                coerce = true;
            }
            assert RelOptUtil.areRowTypesEqual(
                newInputs.get(i).getRowType(),
                expectedType,
                coerceNames);
        }
        if (!coerce) {
            return;
        }
        RelNode newConsumerRel =
            consumerRel.copy(
                consumerRel.getTraitSet(),
                newInputs);
        call.transformTo(newConsumerRel);
    }
}

// End CoerceInputsRule.java
