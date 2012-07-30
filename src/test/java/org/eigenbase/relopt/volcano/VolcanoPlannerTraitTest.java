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
package org.eigenbase.relopt.volcano;

import java.util.List;

import junit.framework.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * VolcanoPlannerTraitTest
 *
 * @author Stephan Zuercher
 */
public class VolcanoPlannerTraitTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Private calling convention representing a generic "physical" calling
     * convention.
     */
    private static final CallingConvention PHYS_CALLING_CONVENTION =
        new CallingConvention(
            "PHYS",
            CallingConvention.generateOrdinal(),
            RelNode.class);

    /**
     * Private trait definition for an alternate type of traits.
     */
    private static final AltTraitDef ALT_TRAIT_DEF = new AltTraitDef();

    /**
     * Private alternate trait.
     */
    private static final AltTrait ALT_TRAIT =
        new AltTrait(ALT_TRAIT_DEF, "ALT");

    /**
     * Private alternate trait.
     */
    private static final AltTrait ALT_TRAIT2 =
        new AltTrait(ALT_TRAIT_DEF, "ALT2");

    /**
     * Ordinal count for alternate traits (so they can implement equals() and
     * avoid being canonized into the same trait).
     */
    private static int altTraitOrdinal = 0;

    //~ Constructors -----------------------------------------------------------

    public VolcanoPlannerTraitTest(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public void testDoubleConversion()
    {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRelTraitDef(CallingConventionTraitDef.instance);
        planner.addRelTraitDef(ALT_TRAIT_DEF);

        planner.addRule(new PhysToIteratorConverterRule());
        planner.addRule(
            new AltTraitConverterRule(
                ALT_TRAIT,
                ALT_TRAIT2,
                "AltToAlt2ConverterRule"));
        planner.addRule(new PhysLeafRule());
        planner.addRule(new IterSingleRule());

        RelOptCluster cluster = VolcanoPlannerTest.newCluster(planner);

        NoneLeafRel noneLeafRel =
            RelOptUtil.addTrait(
                new NoneLeafRel(cluster, "noneLeafRel"), ALT_TRAIT);

        NoneSingleRel noneRel =
            RelOptUtil.addTrait(
                new NoneSingleRel(cluster, noneLeafRel),
                ALT_TRAIT2);

        RelNode convertedRel =
            planner.changeTraits(
                noneRel,
                cluster.traitSetOf(CallingConvention.ITERATOR, ALT_TRAIT2));

        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();

        assertTrue(result instanceof IterSingleRel);
        assertEquals(
            CallingConvention.ITERATOR,
            result.getTraitSet().getTrait(CallingConventionTraitDef.instance));
        assertEquals(
            ALT_TRAIT2,
            result.getTraitSet().getTrait(ALT_TRAIT_DEF));

        RelNode child = result.getInputs().get(0);
        assertTrue(
            (child instanceof AltTraitConverter)
            || (child instanceof PhysToIteratorConverter));

        child = child.getInputs().get(0);
        assertTrue(
            (child instanceof AltTraitConverter)
            || (child instanceof PhysToIteratorConverter));

        child = child.getInputs().get(0);
        assertTrue(child instanceof PhysLeafRel);
    }

    public void testTraitPropagation()
    {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRelTraitDef(CallingConventionTraitDef.instance);
        planner.addRelTraitDef(ALT_TRAIT_DEF);

        planner.addRule(new PhysToIteratorConverterRule());
        planner.addRule(
            new AltTraitConverterRule(
                ALT_TRAIT,
                ALT_TRAIT2,
                "AltToAlt2ConverterRule"));
        planner.addRule(new PhysLeafRule());
        planner.addRule(new IterSingleRule2());

        RelOptCluster cluster = VolcanoPlannerTest.newCluster(planner);

        NoneLeafRel noneLeafRel =
            RelOptUtil.addTrait(
                new NoneLeafRel(cluster, "noneLeafRel"),
                ALT_TRAIT);

        NoneSingleRel noneRel =
            RelOptUtil.addTrait(
                new NoneSingleRel(cluster, noneLeafRel),
                ALT_TRAIT2);

        RelNode convertedRel =
            planner.changeTraits(
                noneRel,
                cluster.traitSetOf(
                    CallingConvention.ITERATOR, ALT_TRAIT2));

        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();

        assertTrue(result instanceof IterSingleRel);
        assertEquals(
            CallingConvention.ITERATOR,
            result.getTraitSet().getTrait(CallingConventionTraitDef.instance));
        assertEquals(
            ALT_TRAIT2,
            result.getTraitSet().getTrait(ALT_TRAIT_DEF));

        RelNode child = result.getInputs().get(0);
        assertTrue(child instanceof IterSingleRel);
        assertEquals(
            CallingConvention.ITERATOR,
            child.getTraitSet().getTrait(CallingConventionTraitDef.instance));
        assertEquals(
            ALT_TRAIT2,
            child.getTraitSet().getTrait(ALT_TRAIT_DEF));

        child = child.getInputs().get(0);
        assertTrue(
            (child instanceof AltTraitConverter)
            || (child instanceof PhysToIteratorConverter));

        child = child.getInputs().get(0);
        assertTrue(
            (child instanceof AltTraitConverter)
            || (child instanceof PhysToIteratorConverter));

        child = child.getInputs().get(0);
        assertTrue(child instanceof PhysLeafRel);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class AltTrait
        implements RelTrait
    {
        private final AltTraitDef traitDef;
        private final int ordinal;
        private final String description;

        private AltTrait(AltTraitDef traitDef, String description)
        {
            this.traitDef = traitDef;
            this.description = description;
            this.ordinal = altTraitOrdinal++;
        }

        public RelTraitDef getTraitDef()
        {
            return traitDef;
        }

        public boolean equals(Object other)
        {
            if (other == null) {
                return false;
            }

            AltTrait that = (AltTrait) other;
            return this.ordinal == that.ordinal;
        }

        public int hashCode()
        {
            return ordinal;
        }

        public String toString()
        {
            return description;
        }
    }

    private static class AltTraitDef
        extends RelTraitDef
    {
        private MultiMap<RelTrait, Pair<RelTrait, ConverterRule>>
            conversionMap =
                new MultiMap<RelTrait, Pair<RelTrait, ConverterRule>>();

        public Class getTraitClass()
        {
            return AltTrait.class;
        }

        public String getSimpleName()
        {
            return "alt_phys";
        }

        public RelNode convert(
            RelOptPlanner planner,
            RelNode rel,
            RelTrait toTrait,
            boolean allowInfiniteCostConverters)
        {
            RelTrait fromTrait = rel.getTraitSet().getTrait(this);

            if (conversionMap.containsKey(fromTrait)) {
                for (
                    Pair<RelTrait, ConverterRule> traitAndRule
                    : conversionMap.getMulti(fromTrait))
                {
                    RelTrait trait = traitAndRule.left;
                    ConverterRule rule = traitAndRule.right;

                    if (trait == toTrait) {
                        RelNode converted = rule.convert(rel);
                        if ((converted != null)
                            && (!planner.getCost(converted).isInfinite()
                                || allowInfiniteCostConverters))
                        {
                            return converted;
                        }
                    }
                }
            }

            return null;
        }

        public boolean canConvert(
            RelOptPlanner planner,
            RelTrait fromTrait,
            RelTrait toTrait)
        {
            if (conversionMap.containsKey(fromTrait)) {
                for (
                    Pair<RelTrait, ConverterRule> traitAndRule
                    : conversionMap.getMulti(fromTrait))
                {
                    if (traitAndRule.left == toTrait) {
                        return true;
                    }
                }
            }

            return false;
        }

        public void registerConverterRule(
            RelOptPlanner planner,
            ConverterRule converterRule)
        {
            if (!converterRule.isGuaranteed()) {
                return;
            }

            RelTrait fromTrait = converterRule.getInTrait();
            RelTrait toTrait = converterRule.getOutTrait();

            conversionMap.putMulti(
                fromTrait,
                new Pair<RelTrait, ConverterRule>(toTrait, converterRule));
        }
    }

    private static abstract class TestLeafRel
        extends AbstractRelNode
    {
        private String label;

        protected TestLeafRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            String label)
        {
            super(cluster, traits);
            this.label = label;
        }

        public String getLabel()
        {
            return label;
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeInfiniteCost();
        }

        // implement RelNode
        protected RelDataType deriveRowType()
        {
            return getCluster().getTypeFactory().createStructType(
                new RelDataType[] {
                    getCluster().getTypeFactory().createJavaType(Void.TYPE)
                },
                new String[] { "this" });
        }

        public void explain(RelOptPlanWriter pw)
        {
            pw.explain(
                this,
                new String[] { "label" },
                new Object[] { label });
        }
    }

    private static class NoneLeafRel
        extends TestLeafRel
    {
        protected NoneLeafRel(
            RelOptCluster cluster,
            String label)
        {
            super(
                cluster,
                cluster.traitSetOf(CallingConvention.NONE),
                label);
        }
    }

    private static class PhysLeafRel
        extends TestLeafRel
    {
        PhysLeafRel(
            RelOptCluster cluster,
            String label)
        {
            super(
                cluster,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION),
                label);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeTinyCost();
        }

        // TODO: SWZ Implement clone?
    }

    private static abstract class TestSingleRel
        extends SingleRel
    {
        protected TestSingleRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child)
        {
            super(cluster, traits, child);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeInfiniteCost();
        }

        // implement RelNode
        protected RelDataType deriveRowType()
        {
            return getChild().getRowType();
        }

        // TODO: SWZ Implement clone?
    }

    private static class NoneSingleRel
        extends TestSingleRel
    {
        protected NoneSingleRel(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                cluster.traitSetOf(CallingConvention.NONE),
                child);
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert traitSet.comprises(CallingConvention.NONE);
            return new NoneSingleRel(
                getCluster(),
                sole(inputs));
        }
    }

    private static class IterSingleRel
        extends TestSingleRel
        implements JavaRel
    {
        public IterSingleRel(RelOptCluster cluster, RelNode child)
        {
            super(
                cluster,
                cluster.traitSetOf(CallingConvention.ITERATOR),
                child);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeTinyCost();
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert traitSet.comprises(CallingConvention.ITERATOR);
            return new IterSingleRel(
                getCluster(),
                sole(inputs));
        }

        public ParseTree implement(JavaRelImplementor implementor)
        {
            return null;
        }
    }

    private static class PhysLeafRule
        extends RelOptRule
    {
        PhysLeafRule()
        {
            super(new RelOptRuleOperand(NoneLeafRel.class, ANY));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneLeafRel leafRel = (NoneLeafRel) call.rels[0];
            call.transformTo(
                new PhysLeafRel(
                    leafRel.getCluster(),
                    leafRel.getLabel()));
        }
    }

    private static class IterSingleRule
        extends RelOptRule
    {
        IterSingleRule()
        {
            super(new RelOptRuleOperand(NoneSingleRel.class, ANY));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return CallingConvention.ITERATOR;
        }

        public RelTrait getOutTrait()
        {
            return getOutConvention();
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel rel = (NoneSingleRel) call.rels[0];

            RelNode converted =
                mergeTraitsAndConvert(
                    rel.getTraitSet(),
                    getOutTrait(),
                    rel.getInput(0));

            call.transformTo(
                new IterSingleRel(
                    rel.getCluster(),
                    converted));
        }
    }

    private static class IterSingleRule2
        extends RelOptRule
    {
        IterSingleRule2()
        {
            super(new RelOptRuleOperand(NoneSingleRel.class, ANY));
        }

        // implement RelOptRule
        public CallingConvention getOutConvention()
        {
            return CallingConvention.ITERATOR;
        }

        public RelTrait getOutTrait()
        {
            return getOutConvention();
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel rel = (NoneSingleRel) call.rels[0];

            RelNode converted =
                mergeTraitsAndConvert(
                    rel.getTraitSet(),
                    getOutTrait(),
                    rel.getInput(0));

            IterSingleRel child =
                new IterSingleRel(
                    rel.getCluster(),
                    converted);

            call.transformTo(
                new IterSingleRel(
                    rel.getCluster(),
                    child));
        }
    }

    private static class AltTraitConverterRule
        extends ConverterRule
    {
        private final RelTrait toTrait;

        private AltTraitConverterRule(
            AltTrait fromTrait,
            AltTrait toTrait,
            String description)
        {
            super(
                RelNode.class,
                fromTrait,
                toTrait,
                description);

            this.toTrait = toTrait;
        }

        public RelNode convert(RelNode rel)
        {
            return new AltTraitConverter(
                rel.getCluster(),
                rel,
                toTrait);
        }

        public boolean isGuaranteed()
        {
            return true;
        }
    }

    private static class AltTraitConverter
        extends ConverterRelImpl
    {
        private final RelTrait toTrait;

        private AltTraitConverter(
            RelOptCluster cluster,
            RelNode child,
            RelTrait toTrait)
        {
            super(
                cluster,
                toTrait.getTraitDef(),
                child.getTraitSet().replace(toTrait),
                child);

            this.toTrait = toTrait;
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new AltTraitConverter(
                getCluster(),
                sole(inputs),
                toTrait);
        }
    }

    private static class PhysToIteratorConverterRule
        extends ConverterRule
    {
        public PhysToIteratorConverterRule()
        {
            super(
                RelNode.class,
                PHYS_CALLING_CONVENTION,
                CallingConvention.ITERATOR,
                "PhysToIteratorRule");
        }

        public RelNode convert(RelNode rel)
        {
            return new PhysToIteratorConverter(
                rel.getCluster(),
                rel);
        }
    }

    private static class PhysToIteratorConverter
        extends ConverterRelImpl
    {
        public PhysToIteratorConverter(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                CallingConventionTraitDef.instance,
                child.getTraitSet().replace(CallingConvention.ITERATOR),
                child);
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PhysToIteratorConverter(
                getCluster(),
                sole(inputs));
        }
    }
}

// End VolcanoPlannerTraitTest.java
