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
 * Abstract base class for a rule which converts from one calling convention to
 * another without changing semantics.
 */
public abstract class ConverterRule
    extends RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    private final RelTrait inTrait;
    private final RelTrait outTrait;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>ConverterRule</code>.
     *
     * @param clazz Type of relational expression to consider converting
     * @param in Trait of relational expression to consider converting
     * @param out Trait which is converted to
     * @param description Description of rule
     *
     * @pre in != null
     * @pre out != null
     */
    public ConverterRule(
        Class<? extends RelNode> clazz,
        RelTrait in,
        RelTrait out,
        String description)
    {
        super(
            new ConverterRelOptRuleOperand(clazz, in),
            description == null
                ? "ConverterRule<in=" + in + ",out=" + out + ">"
                : description);
        assert (in != null);
        assert (out != null);

        // Source and target traits must have same type
        assert in.getTraitDef() == out.getTraitDef();

        this.inTrait = in;
        this.outTrait = out;
    }

    //~ Methods ----------------------------------------------------------------

    public Convention getOutConvention()
    {
        return (Convention) outTrait;
    }

    public RelTrait getOutTrait()
    {
        return outTrait;
    }

    public RelTrait getInTrait()
    {
        return inTrait;
    }

    public RelTraitDef getTraitDef()
    {
        return inTrait.getTraitDef();
    }

    public abstract RelNode convert(RelNode rel);

    /**
     * Returns true if this rule can convert <em>any</em> relational expression
     * of the input convention.
     *
     * <p>The union-to-java converter, for example, is not guaranteed, because
     * it only works on unions.</p>
     */
    public boolean isGuaranteed()
    {
        return false;
    }

    public void onMatch(RelOptRuleCall call)
    {
        RelNode rel = call.rel(0);
        if (rel.getTraitSet().contains(inTrait)) {
            final RelNode converted = convert(rel);
            if (converted != null) {
                call.transformTo(converted);
            }
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class ConverterRelOptRuleOperand
        extends RelOptRuleOperand
    {
        public ConverterRelOptRuleOperand(
            Class<? extends RelNode> clazz, RelTrait in)
        {
            super(clazz, in, RelOptRule.ANY, null);
        }

        public boolean matches(RelNode rel)
        {
            // Don't apply converters to converters that operate
            // on the same RelTraitDef -- otherwise we get
            // an n^2 effect.
            if (rel instanceof ConverterRel) {
                if (((ConverterRule) getRule()).getTraitDef()
                    == ((ConverterRel) rel).getTraitDef())
                {
                    return false;
                }
            }
            return super.matches(rel);
        }
    }
}

// End ConverterRule.java
