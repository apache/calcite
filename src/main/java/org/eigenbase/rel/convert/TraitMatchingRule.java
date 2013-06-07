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
package org.eigenbase.rel.convert;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * TraitMatchingRule adapts a converter rule, restricting it to fire only when
 * its input already matches the expected output trait. This can be used with
 * {@link org.eigenbase.relopt.hep.HepPlanner} in cases where alternate
 * implementations are available and it is desirable to minimize converters.
 */
public class TraitMatchingRule
    extends RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    private final ConverterRule converter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new TraitMatchingRule.
     *
     * @param converterRule rule to be restricted; rule must take a single
     * operand expecting a single input
     */
    public TraitMatchingRule(ConverterRule converterRule)
    {
        super(
            some(
                converterRule.getOperand().getMatchedClass(),
                any(RelNode.class)),
            "TraitMatchingRule: " + converterRule);
        assert (converterRule.getOperand().getChildOperands() == null);
        this.converter = converterRule;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public Convention getOutConvention()
    {
        return converter.getOutConvention();
    }

    public void onMatch(RelOptRuleCall call)
    {
        RelNode input = call.rel(1);
        if (input.getTraitSet().contains(converter.getOutTrait())) {
            converter.onMatch(call);
        }
    }
}

// End TraitMatchingRule.java
